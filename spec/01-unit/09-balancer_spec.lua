local utils = require "kong.tools.utils"
local mocker = require "spec.fixtures.mocker"

local ws_id = utils.uuid()

local table_clear = require "table.clear"
local function table_merge(a, b, ...)
  if not b then
    return a
  end

  for k, v in pairs(b) do
    a[k] = v
  end
  return table_merge(a, ...)
end

local function setup_it_block(consistency)
  local cache_table = {}

  local function mock_cache(cache_table, limit)
    return {
      safe_set = function(self, k, v)
        if limit then
          local n = 0
          for _, _ in pairs(cache_table) do
            n = n + 1
          end
          if n >= limit then
            return nil, "no memory"
          end
        end
        cache_table[k] = v
        return true
      end,
      get = function(self, k, _, fn, arg)
        if cache_table[k] == nil then
          cache_table[k] = fn(arg)
        end
        return cache_table[k]
      end,
    }
  end

  mocker.setup(finally, {
    kong = {
      configuration = {
        worker_consistency = consistency,
        worker_state_update_frequency = 0.1,
      },
      core_cache = mock_cache(cache_table),
    },
    ngx = {
      ctx = {
        workspace = ws_id,
      }
    }
  })
end

for _, consistency in ipairs({"strict", "eventual"}) do
  describe("Balancer (worker_consistency = " .. consistency .. ")", function()
    local singletons, balancer
    local targets, upstreams, balancers, healthcheckers
    local dns_utils
    local UPSTREAMS_FIXTURES
    local TARGETS_FIXTURES
    local upstream_hc
    local upstream_ph

    lazy_teardown(function()
      ngx.log:revert() -- luacheck: ignore
    end)


    lazy_setup(function()
      stub(ngx, "log")

      balancer = require "kong.runloop.balancer"
      singletons = require "kong.singletons"
      singletons.worker_events = require "resty.worker.events"
      singletons.db = {}
      targets = require "kong.runloop.balancer.targets"
      upstreams = require "kong.runloop.balancer.upstreams"
      balancers = require "kong.runloop.balancer.balancers"
      healthcheckers = require "kong.runloop.balancer.healthcheckers"
      dns_utils = require "resty.dns.utils"

      singletons.worker_events.configure({
        shm = "kong_process_events", -- defined by "lua_shared_dict"
        timeout = 5,            -- life time of event data in shm
        interval = 1,           -- poll interval (seconds)

        wait_interval = 0.010,  -- wait before retry fetching event data
        wait_max = 0.5,         -- max wait time before discarding event
      })

      local hc_defaults = {
        active = {
          timeout = 1,
          concurrency = 10,
          http_path = "/",
          healthy = {
            interval = 0,  -- 0 = probing disabled by default
            http_statuses = { 200, 302 },
            successes = 0, -- 0 = disabled by default
          },
          unhealthy = {
            interval = 0, -- 0 = probing disabled by default
            http_statuses = { 429, 404,
                              500, 501, 502, 503, 504, 505 },
            tcp_failures = 0,  -- 0 = disabled by default
            timeouts = 0,      -- 0 = disabled by default
            http_failures = 0, -- 0 = disabled by default
          },
        },
        passive = {
          healthy = {
            http_statuses = { 200, 201, 202, 203, 204, 205, 206, 207, 208, 226,
                              300, 301, 302, 303, 304, 305, 306, 307, 308 },
            successes = 0,
          },
          unhealthy = {
            http_statuses = { 429, 500, 503 },
            tcp_failures = 0,  -- 0 = circuit-breaker disabled by default
            timeouts = 0,      -- 0 = circuit-breaker disabled by default
            http_failures = 0, -- 0 = circuit-breaker disabled by default
          },
        },
      }

      local passive_hc = utils.deep_copy(hc_defaults)
      passive_hc.passive.healthy.successes = 1
      passive_hc.passive.unhealthy.http_failures = 1

      UPSTREAMS_FIXTURES = {
        [1] = { id = "a", ws_id = ws_id, name = "mashape", slots = 10, healthchecks = hc_defaults, algorithm = "round-robin" },
        [2] = { id = "b", ws_id = ws_id, name = "kong",    slots = 10, healthchecks = hc_defaults, algorithm = "round-robin" },
        [3] = { id = "c", ws_id = ws_id, name = "gelato",  slots = 20, healthchecks = hc_defaults, algorithm = "round-robin" },
        [4] = { id = "d", ws_id = ws_id, name = "galileo", slots = 20, healthchecks = hc_defaults, algorithm = "round-robin" },
        [5] = { id = "e", ws_id = ws_id, name = "upstream_e", slots = 10, healthchecks = hc_defaults, algorithm = "round-robin" },
        [6] = { id = "f", ws_id = ws_id, name = "upstream_f", slots = 10, healthchecks = hc_defaults, algorithm = "round-robin" },
        [7] = { id = "hc_" .. consistency, ws_id = ws_id, name = "upstream_hc_" .. consistency, slots = 10, healthchecks = passive_hc, algorithm = "round-robin" },
        [8] = { id = "ph", ws_id = ws_id, name = "upstream_ph", slots = 10, healthchecks = passive_hc, algorithm = "round-robin" },
        [9] = { id = "otes", ws_id = ws_id, name = "upstream_otes", slots = 10, healthchecks = hc_defaults, algorithm = "round-robin" },
        [10] = { id = "otee", ws_id = ws_id, name = "upstream_otee", slots = 10, healthchecks = hc_defaults, algorithm = "round-robin" },
      }
      upstream_hc = UPSTREAMS_FIXTURES[7]
      upstream_ph = UPSTREAMS_FIXTURES[8]

      TARGETS_FIXTURES = {
        -- 1st upstream; a
        {
          id = "a1",
          ws_id = ws_id,
          created_at = "003",
          upstream = { id = "a", ws_id = ws_id },
          target = "localhost:80",
          weight = 10,
        },
        {
          id = "a2",
          ws_id = ws_id,
          created_at = "002",
          upstream = { id = "a", ws_id = ws_id },
          target = "localhost:80",
          weight = 10,
        },
        {
          id = "a3",
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "a", ws_id = ws_id },
          target = "localhost:80",
          weight = 10,
        },
        {
          id = "a4",
          ws_id = ws_id,
          created_at = "002",  -- same timestamp as "a2"
          upstream = { id = "a", ws_id = ws_id },
          target = "localhost:80",
          weight = 10,
        },
        -- 2nd upstream; b
        {
          id = "b1",
          ws_id = ws_id,
          created_at = "003",
          upstream = { id = "b", ws_id = ws_id },
          target = "localhost:80",
          weight = 10,
        },
        -- 3rd upstream: e (removed and re-added)
        {
          id = "e1",
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "e", ws_id = ws_id },
          target = "127.0.0.1:2112",
          weight = 10,
        },
        {
          id = "e2",
          ws_id = ws_id,
          created_at = "002",
          upstream = { id = "e", ws_id = ws_id },
          target = "127.0.0.1:2112",
          weight = 0,
        },
        {
          id = "e3",
          ws_id = ws_id,
          created_at = "003",
          upstream = { id = "e", ws_id = ws_id },
          target = "127.0.0.1:2112",
          weight = 10,
        },
        -- 4th upstream: f (removed and not re-added)
        {
          id = "f1",
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "f", ws_id = ws_id },
          target = "127.0.0.1:5150",
          weight = 10,
        },
        {
          id = "f2",
          ws_id = ws_id,
          created_at = "002",
          upstream = { id = "f", ws_id = ws_id },
          target = "127.0.0.1:5150",
          weight = 0,
        },
        {
          id = "f3",
          ws_id = ws_id,
          created_at = "003",
          upstream = { id = "f", ws_id = ws_id },
          target = "127.0.0.1:2112",
          weight = 10,
        },
        -- upstream_hc
        {
          id = "hc1" .. consistency,
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "hc_" .. consistency, ws_id = ws_id },
          target = "localhost:1111",
          weight = 10,
        },
        -- upstream_ph
        {
          id = "ph1",
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "ph", ws_id = ws_id },
          target = "localhost:1111",
          weight = 10,
        },
        {
          id = "ph2",
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "ph", ws_id = ws_id },
          target = "127.0.0.1:2222",
          weight = 10,
        },
        -- upstream_otes
        {
          id = "otes1",
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "otes", ws_id = ws_id },
          target = "localhost:1111",
          weight = 10,
        },
        -- upstream_otee
        {
          id = "otee1",
          ws_id = ws_id,
          created_at = "001",
          upstream = { id = "otee", ws_id = ws_id },
          target = "localhost:1111",
          weight = 10,
        },
      }

      local function each(fixture)
        return function()
          local i = 0
          return function(self)
            i = i + 1
            return fixture[i]
          end
        end
      end

      local function select(fixture)
        return function(self, pk)
          for item in self:each() do
            if item.id == pk.id then
              return item
            end
          end
        end
      end

      singletons.db = {
        targets = {
          each = each(TARGETS_FIXTURES),
          select_by_upstream_raw = function(self, upstream_pk)
            local upstream_id = upstream_pk.id
            local res, len = {}, 0
            for tgt in self:each() do
              if tgt.upstream.id == upstream_id then
                tgt.order = string.format("%d:%s", tgt.created_at * 1000, tgt.id)
                len = len + 1
                res[len] = tgt
              end
            end

            table.sort(res, function(a, b) return a.order < b.order end)
            return res
          end
        },
        upstreams = {
          each = each(UPSTREAMS_FIXTURES),
          select = select(UPSTREAMS_FIXTURES),
        },
      }

      singletons.core_cache = {
        _cache = {},
        get = function(self, key, _, loader, arg)
          local v = self._cache[key]
          if v == nil then
            v = loader(arg)
            self._cache[key] = v
          end
          return v
        end,
        invalidate_local = function(self, key)
          self._cache[key] = nil
        end
      }

      balancers.init()
      healthcheckers.init()

    end)

    describe("create_balancer()", function()
      local dns_client = require("resty.dns.client")
      dns_client.init()

      it("creates a balancer with a healthchecker", function()
        setup_it_block(consistency)
        local my_balancer = assert(balancers.create_balancer(UPSTREAMS_FIXTURES[1]))
        local hc = assert(my_balancer.healthchecker)
        hc:stop()
      end)

      it("reuses a balancer by default", function()
        local b1 = assert(balancers.create_balancer(UPSTREAMS_FIXTURES[1]))
        local hc1 = b1.healthchecker
        local b2 = balancers.create_balancer(UPSTREAMS_FIXTURES[1])
        assert.equal(b1, b2)
        assert(hc1:stop())
      end)

      it("re-creates a balancer if told to", function()
        setup_it_block(consistency)
        balancer.init()
        local b1 = assert(balancers.create_balancer(UPSTREAMS_FIXTURES[1], true))
        assert(b1.healthchecker:stop())
        local b2 = assert(balancers.create_balancer(UPSTREAMS_FIXTURES[1], true))
        assert(b2.healthchecker:stop())
        assert.not_same(b1, b2)
      end)
    end)

    describe("get_balancer()", function()
      local dns_client = require("resty.dns.client")
      dns_client.init()

      it("balancer and healthchecker match; remove and re-add", function()
        setup_it_block(consistency)
        local my_balancer = assert(balancers.get_balancer({
          host = "upstream_e"
        }, true))
        local hc = assert(my_balancer.healthchecker)
        assert.same(1, #hc.targets)
        assert.truthy(hc.targets["127.0.0.1"])
        assert.truthy(hc.targets["127.0.0.1"][2112])
      end)

      it("balancer and healthchecker match; remove and not re-add", function()
        pending()
        setup_it_block(consistency)
        local my_balancer = assert(balancers.get_balancer({
          host = "upstream_f"
        }, true))
        local hc = assert(my_balancer.healthchecker)
        assert.same(1, #hc.targets)
        assert.truthy(hc.targets["127.0.0.1"])
        assert.truthy(hc.targets["127.0.0.1"][2112])
      end)
    end)

    describe("load_upstreams_dict_into_memory()", function()
      local upstreams_dict
      lazy_setup(function()
        upstreams_dict = upstreams.get_all_upstreams()
      end)

      it("retrieves all upstreams as a dictionary", function()
        assert.is.table(upstreams_dict)
        for _, u in ipairs(UPSTREAMS_FIXTURES) do
          assert.equal(upstreams_dict[ws_id .. ":" .. u.name], u.id)
          upstreams_dict[ws_id .. ":" .. u.name] = nil -- remove each match
        end
        assert.is_nil(next(upstreams_dict)) -- should be empty now
      end)
    end)

    describe("get_all_upstreams()", function()
      it("gets a map of all upstream names to ids", function()
        pending("too implementation dependent")
        setup_it_block(consistency)
        local upstreams_dict = upstreams.get_all_upstreams()

        local fixture_dict = {}
        for _, upstream in ipairs(UPSTREAMS_FIXTURES) do
          fixture_dict[ws_id .. ":" .. upstream.name] = upstream.id
        end

        assert.same(fixture_dict, upstreams_dict)
      end)
    end)

    describe("get_upstream_by_name()", function()
      it("retrieves a complete upstream based on its name", function()
        setup_it_block(consistency)
        for _, fixture in ipairs(UPSTREAMS_FIXTURES) do
          local upstream = balancer.get_upstream_by_name(fixture.name)
          assert.same(fixture, upstream)
        end
      end)
    end)

    describe("load_targets_into_memory()", function()
      local targets_for_upstream_a

      it("retrieves all targets per upstream, ordered", function()
        setup_it_block(consistency)
        targets_for_upstream_a = targets.fetch_targets({ id = "a"})
        assert.equal(4, #targets_for_upstream_a)
        assert(targets_for_upstream_a[1].id == "a3")
        assert(targets_for_upstream_a[2].id == "a2")
        assert(targets_for_upstream_a[3].id == "a4")
        assert(targets_for_upstream_a[4].id == "a1")
      end)
    end)

    describe("post_health()", function()
      local hc, my_balancer

      lazy_setup(function()
        my_balancer = assert(balancers.create_balancer(upstream_ph))
        hc = assert(my_balancer.healthchecker)
      end)

      lazy_teardown(function()
        if hc then
          hc:stop()
        end
      end)

      it("posts healthy/unhealthy using IP and hostname", function()
        setup_it_block(consistency)
        local tests = {
          { host = "127.0.0.1", port = 2222, health = true },
          { host = "127.0.0.1", port = 2222, health = false },
          { host = "localhost", port = 1111, health = true },
          { host = "localhost", port = 1111, health = false },
        }
        for _, t in ipairs(tests) do
          assert(balancer.post_health(upstream_ph, t.host, nil, t.port, t.health))
          local health_info = assert(balancer.get_upstream_health("ph"))
          local response = t.health and "HEALTHY" or "UNHEALTHY"
          assert.same(response,
                      health_info[t.host .. ":" .. t.port].addresses[1].health)
        end
      end)

      it("fails if upstream/balancer doesn't exist", function()
        local bad = { name = "invalid", id = "bad" }
        local ok, err = balancer.post_health(bad, "127.0.0.1", 1111, true)
        assert.falsy(ok)
        assert.match(err, "Upstream invalid has no balancer")
      end)
    end)

    describe("healthcheck events", function()
      it("(un)subscribe_to_healthcheck_events()", function()
        setup_it_block(consistency)
        local my_balancer = assert(balancers.create_balancer(upstream_hc))
        local hc = assert(my_balancer.healthchecker)
        local data = {}
        local cb = function(upstream_id, ip, port, hostname, health)
          table.insert(data, {
            upstream_id = upstream_id,
            ip = ip,
            port = port,
            hostname = hostname,
            health = health,
          })
        end
        balancer.subscribe_to_healthcheck_events(cb)
        my_balancer.report_http_status({
          address = {
            ip = "127.0.0.1",
            port = 1111,
            target = {name = "localhost"},
          }}, 429)
        my_balancer.report_http_status({
          address = {
            ip = "127.0.0.1",
            port = 1111,
            target = {name = "localhost"},
          }}, 200)
        balancer.unsubscribe_from_healthcheck_events(cb)
        my_balancer.report_http_status({
          address = {
            ip = "127.0.0.1",
            port = 1111,
            target = {name = "localhost"},
          }}, 429)
        hc:stop()
        assert.same({
          upstream_id = "hc_" .. consistency,
          ip = "127.0.0.1",
          port = 1111,
          hostname = "localhost",
          health = "unhealthy"
        }, data[1])
        assert.same({
          upstream_id = "hc_" .. consistency,
          ip = "127.0.0.1",
          port = 1111,
          hostname = "localhost",
          health = "healthy"
        }, data[2])
        assert.same(nil, data[3])
      end)
    end)

    local upstream_index = 0
    for _, algorithm in ipairs{ "consistent-hashing", "least-connections", "round-robin" } do

      --local function new_balancer(addresses, target)
      --  setup_it_block(consistency)
      --
      --  upstream_index = upstream_index + 1
      --  local upname="upstream_" .. upstream_index
      --  local hc_defaults = UPSTREAMS_FIXTURES[1].healthchecks
      --  local my_upstream = { id=upname, name=upname, ws_id=ws_id, slots=10, healthchecks=hc_defaults, algorithm=algorithm }
      --  local b = assert(balancers.create_balancer(my_upstream, true))
      --
      --  local first_addr = addresses and addresses[1]
      --  local hostname, port = first_addr, 80
      --  if type(first_addr) == "table" then
      --    hostname = first_addr.address or first_addr.target
      --    port = first_addr.port
      --  end
      --
      --  local t = table_merge({
      --    upstream = upname,
      --    balancer = b,
      --    name = hostname or "nowhere.moc",
      --    addresses = {},
      --    port = port,
      --    weight = 100,
      --    totalWeight = 0,
      --    unavailableWeight = 0,
      --  }, target)
      --  table.insert(b.targets, t)
      --
      --  for _, host in ipairs (addresses or {}) do
      --    local entry = type(host) == "string" and { address = host } or host
      --    assert(b:addAddress(t, entry))
      --  end
      --  return b
      --end

      local function add_target(b, target, addrlist)
        local addrname = addrlist and addrlist[1]
        if type(addrname) == "table" then
          addrname = addrname and (addrname.name or addrname.address)
        end
        local upname = b.upstream and b.upstream.name or b.upstream_id
        local t = table_merge({
          upstream = upname,
          balancer = b,
          name = addrname or "nowhere.moc",
          nameType = dns_utils.hostnameType(addrname or "nowhere.moc"),
          addresses = {},
          port = 8000,
          weight = 100,
          totalWeight = 0,
          unavailableWeight = 0,
        }, target or {})
        table.insert(b.targets, t)

        return t
      end

      local function add_address(b, addr)
        local addrname, addrentry
        if type(addr) == "string" then
          addrname = addr
          addrentry = { address = addr }
        else
          addrentry = addr
          addrname = addr.address or addr.name
        end
        local t = assert(b.targets[#b.targets] or add_target(b, { name = addrname }))
        assert(b:addAddress(t, addrentry))
        return t.addresses[#t.addresses]
      end

      local function new_balancer(addrlist)
        setup_it_block(consistency)

        upstream_index = upstream_index + 1
        local upname="upstream_" .. upstream_index
        local hc_defaults = UPSTREAMS_FIXTURES[1].healthchecks
        local my_upstream = { id=upname, name=upname, ws_id=ws_id, slots=10, healthchecks=hc_defaults, algorithm=algorithm }
        local b = (balancers.create_balancer(my_upstream, true))

        for _, addr in ipairs(addrlist or {}) do
          add_address(b, addr)
        end

        return b
      end


      describe("[ #tmp " .. algorithm .."]", function()
        local dnsA, dnsSRV
        local dns_helpers = require "spec.helpers.dns"
        setup(function()
          package.loaded["resty.dns.client"] = nil
          local dns_client = require "resty.dns.client"
          dns_client.init()
          --local dns_client = require "kong.tools.dns"()
          function dnsA(...) return dns_helpers.dnsA(dns_client, ...) end
          function dnsSRV(...) return dns_helpers.dnsSRV(dns_client, ...) end
        end)

        describe("health", function()
          it("empty balancer is unhealthy", function()
            local b = new_balancer()
            assert.is_false((b:getStatus().healthy))
            add_target(b)
            assert.is_false((b:getStatus().healthy))
          end)

          it("adding first address marks healthy", function()
            local b = new_balancer()
            local t = add_target(b)
            assert.is_false(b:getStatus().healthy)

            assert(b:addAddress(t, {address="127.0.0.1"}))
            assert.is_true(b:getStatus().healthy)
          end)

          it("removing last address marks unhealthy", function()
            local b = new_balancer({"127.0.0.1"})
            assert.is_true(b:getStatus().healthy)

            local target = b.targets[1]
            b:disableAddress(target, {address="127.0.0.1"})
            assert.is_false(b:getStatus().healthy)
          end)

          it("dropping below the health threshold marks unhealthy", function()
            local b = new_balancer({ "127.0.0.1", "127.0.0.2", "127.0.0.3" })
            b.healthThreshold = 50
            assert.is_true(b:getStatus().healthy)

            local addresses = b.targets[1].addresses
            b:setAddressStatus(addresses[1], false)
            assert.is_true(b:getStatus().healthy)

            b:setAddressStatus(addresses[2], false)
            assert.is_false(b:getStatus().healthy)

            -- rising above threshold maks healthy again
            b:setAddressStatus(addresses[1], true)
            assert.is_true(b:getStatus().healthy)
          end)
        end)

        describe("weights", function()
          describe("(A)", function()

            it("basic status", function()
              local b = new_balancer({ "127.0.0.1" })
              targets.resolve_targets(b.targets)
              assert.same({
                healthy = true,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                },
              }, b:getStatus())

            end)

            it("switching availability", function()
              dnsA({
                { name = "arecord.tst", address = "1.2.3.4" },
                { name = "arecord.tst", address = "5.6.7.8" },
              })
              local b = new_balancer({ "127.0.0.1" })
              targets.resolve_targets(b.targets)

              assert.same({
                healthy = true,
                weight = {
                  total = 100,
                  available = 100,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                },
              }, b:getStatus())

              add_target(b, { name = "arecord.tst", port = 8001, weight = 25 })
              targets.resolve_targets(b.targets)

              assert.same({
                healthy = true,
                weight = {
                  total = 150,
                  available = 150,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "arecord.tst",
                    port = 8001,
                    dns = "A",
                    nodeWeight = 25,
                    weight = {
                      total = 50,
                      available = 50,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.2.3.4",
                        port = 8001,
                        weight = 25
                      },
                      {
                        healthy = true,
                        ip = "5.6.7.8",
                        port = 8001,
                        weight = 25
                      },
                    },
                  },
                },
              }, b:getStatus())

              -- switch to unavailable
              b:setAddressStatus(b.targets[2].addresses[1], false)
              assert.same({
                healthy = true,
                weight = {
                  total = 150,
                  available = 125,
                  unavailable = 25
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "arecord.tst",
                    port = 8001,
                    dns = "A",
                    nodeWeight = 25,
                    weight = {
                      total = 50,
                      available = 25,
                      unavailable = 25
                    },
                    addresses = {
                      {
                        healthy = false,
                        ip = "1.2.3.4",
                        port = 8001,
                        weight = 25
                      },
                      {
                        healthy = true,
                        ip = "5.6.7.8",
                        port = 8001,
                        weight = 25
                      },
                    },
                  },
                },
              }, b:getStatus())

              -- switch to available
              b:setAddressStatus(b.targets[2].addresses[1], true)
              assert.same({
                healthy = true,
                weight = {
                  total = 150,
                  available = 150,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "arecord.tst",
                    port = 8001,
                    dns = "A",
                    nodeWeight = 25,
                    weight = {
                      total = 50,
                      available = 50,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.2.3.4",
                        port = 8001,
                        weight = 25
                      },
                      {
                        healthy = true,
                        ip = "5.6.7.8",
                        port = 8001,
                        weight = 25
                      },
                    },
                  },
                },
              }, b:getStatus())

            end)
            it("changing weight of an available address",function()
              dnsA({
                { name = "arecord.tst", address = "1.2.3.4" },
                { name = "arecord.tst", address = "5.6.7.8" },
              })
              local b = new_balancer()
              add_target(b, { name = "127.0.0.1" })
              add_target(b, { name = "arecord.tst", port = 8001, weight = 25 })
              targets.resolve_targets(b.targets)

              assert.same({
                healthy = true,
                weight = {
                  total = 150,
                  available = 150,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "arecord.tst",
                    port = 8001,
                    dns = "A",
                    nodeWeight = 25,
                    weight = {
                      total = 50,
                      available = 50,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.2.3.4",
                        port = 8001,
                        weight = 25
                      },
                      {
                        healthy = true,
                        ip = "5.6.7.8",
                        port = 8001,
                        weight = 25
                      },
                    },
                  },
                },
              }, b:getStatus())

              local t2 = b.targets[2]
              b:disableAddress(t2, { address = "1.2.3.4" })
              b:disableAddress(t2, { address = "5.6.7.8" })
              b:deleteDisabledAddresses(t2)
              b.targets[2] = nil
              add_target(b, { name = "arecord.tst", port = 8001, weight = 50 })
              targets.resolve_targets(b.targets)
              assert.same({
                healthy = true,
                weight = {
                  total = 200,
                  available = 200,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "arecord.tst",
                    port = 8001,
                    dns = "A",
                    nodeWeight = 50,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.2.3.4",
                        port = 8001,
                        weight = 50
                      },
                      {
                        healthy = true,
                        ip = "5.6.7.8",
                        port = 8001,
                        weight = 50
                      },
                    },
                  },
                },
              }, b:getStatus())

            end)
          end)

          describe("(SRV)", function()

            it("adding a host",function()
              dnsSRV({
                { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
                { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
              })

              local b = new_balancer()
              add_target(b, { name = "127.0.0.1" })
              add_target(b, { name = "srvrecord.tst", port = 8001, weight = 25 })
              targets.resolve_targets(b.targets)
              assert.same({
                healthy = true,
                weight = {
                  total = 120,
                  available = 120,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "srvrecord.tst",
                    port = 8001,
                    dns = "SRV",
                    nodeWeight = 25,
                    weight = {
                      total = 20,
                      available = 20,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.1.1.1",
                        port = 9000,
                        weight = 10
                      },
                      {
                        healthy = true,
                        ip = "2.2.2.2",
                        port = 9001,
                        weight = 10
                      },
                    },
                  },
                },
              }, b:getStatus())
            end)

            it("switching address availability",function()
              dnsSRV({
                { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
                { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
              })

              local b = new_balancer()
              add_target(b, { name = "127.0.0.1" })
              add_target(b, { name = "srvrecord.tst", port = 8001, weight = 25 })
              targets.resolve_targets(b.targets)
              assert.same({
                healthy = true,
                weight = {
                  total = 120,
                  available = 120,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "srvrecord.tst",
                    port = 8001,
                    dns = "SRV",
                    nodeWeight = 25,
                    weight = {
                      total = 20,
                      available = 20,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.1.1.1",
                        port = 9000,
                        weight = 10
                      },
                      {
                        healthy = true,
                        ip = "2.2.2.2",
                        port = 9001,
                        weight = 10
                      },
                    },
                  },
                },
              }, b:getStatus())

              -- switch to unavailable
              assert(b:setAddressStatus(b.targets[2].addresses[1], false))
              assert.same({
                healthy = true,
                weight = {
                  total = 120,
                  available = 110,
                  unavailable = 10
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "srvrecord.tst",
                    port = 8001,
                    dns = "SRV",
                    nodeWeight = 25,
                    weight = {
                      total = 20,
                      available = 10,
                      unavailable = 10
                    },
                    addresses = {
                      {
                        healthy = false,
                        ip = "1.1.1.1",
                        port = 9000,
                        weight = 10
                      },
                      {
                        healthy = true,
                        ip = "2.2.2.2",
                        port = 9001,
                        weight = 10
                      },
                    },
                  },
                },
              }, b:getStatus())

              -- switch to available
              assert(b:setAddressStatus(b.targets[2].addresses[1], true))
              assert.same({
                healthy = true,
                weight = {
                  total = 120,
                  available = 120,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "srvrecord.tst",
                    port = 8001,
                    dns = "SRV",
                    nodeWeight = 25,
                    weight = {
                      total = 20,
                      available = 20,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.1.1.1",
                        port = 9000,
                        weight = 10
                      },
                      {
                        healthy = true,
                        ip = "2.2.2.2",
                        port = 9001,
                        weight = 10
                      },
                    },
                  },
                },
              }, b:getStatus())
            end)

            it("changing weight of an available address (dns update)",function()
              pending("skip")
              local record = dnsSRV({
                { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 10 },
                { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 10 },
              })

              local b = new_balancer()
              add_target(b, { name = "127.0.0.1" })
              add_target(b, { name = "srvrecord.tst", port = 8001, weight = 10 })
              targets.resolve_targets(b.targets)
              --b:addHost("srvrecord.tst", 8001, 10)
              assert.same({
                healthy = true,
                weight = {
                  total = 120,
                  available = 120,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "srvrecord.tst",
                    port = 8001,
                    dns = "SRV",
                    nodeWeight = 10,
                    weight = {
                      total = 20,
                      available = 20,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.1.1.1",
                        port = 9000,
                        weight = 10
                      },
                      {
                        healthy = true,
                        ip = "2.2.2.2",
                        port = 9001,
                        weight = 10
                      },
                    },
                  },
                },
              }, b:getStatus())

              dns_helpers.dnsExpire(record)
              --dns_client.getcache():flush_all()
              --b.targets[2].weight = 20
              dnsSRV({
                { name = "srvrecord.tst", target = "1.1.1.1", port = 9000, weight = 20 },
                { name = "srvrecord.tst", target = "2.2.2.2", port = 9001, weight = 20 },
              })
              targets.resolve_targets(b.targets)

              assert.same({
                healthy = true,
                weight = {
                  total = 140,
                  available = 140,
                  unavailable = 0
                },
                hosts = {
                  {
                    host = "127.0.0.1",
                    port = 8000,
                    dns = "A",
                    nodeWeight = 100,
                    weight = {
                      total = 100,
                      available = 100,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "127.0.0.1",
                        port = 8000,
                        weight = 100
                      },
                    },
                  },
                  {
                    host = "srvrecord.tst",
                    port = 8001,
                    dns = "SRV",
                    nodeWeight = 10,
                    weight = {
                      total = 40,
                      available = 40,
                      unavailable = 0
                    },
                    addresses = {
                      {
                        healthy = true,
                        ip = "1.1.1.1",
                        port = 9000,
                        weight = 20
                      },
                      {
                        healthy = true,
                        ip = "2.2.2.2",
                        port = 9001,
                        weight = 20
                      },
                    },
                  },
                },
              }, b:getStatus())
            end)
          end)
        end)

        describe("getpeer()", function()

          before_each(function()
            package.loaded["resty.dns.client"] = nil
            local dns_client = require "resty.dns.client"
            dns_client.init()
            --b = balancer_module.new({
            --  dns = client,
            --  healthThreshold = 50,
            --  useSRVname = false,
            --})
          end)

          after_each(function()
            --b = nil
          end)


          it("returns expected results/types when using SRV with IP", function()
            pending("skip")
            dnsSRV({
            { name = "konghq.com", target = "1.1.1.1", port = 2, weight = 3 },
            })
            local b = new_balancer()
            add_target(b, { name = "konghq.com", port = 8000, weight = 50 })
            targets.resolve_targets(b.targets)
            --b:addHost("konghq.com", 8000, 50)
            local ip, port, hostname, handle = assert(b:getPeer(nil, nil, "xxx"))
            assert.equal("1.1.1.1", ip)
            assert.equal(2, port)
            assert.equal("konghq.com", hostname)
            assert.not_nil(handle)
          end)


          it("returns expected results/types when using SRV with name ('useSRVname=false')", function()
            --pending("skip")
            dnsA({
            { name = "getkong.org", address = "1.2.3.4" },
            })
            dnsSRV({
            { name = "konghq.com", target = "getkong.org", port = 2, weight = 3 },
            })
            local b = new_balancer()
            add_target(b, { name = "konghq.com", port = 8000, weight = 50 })
            targets.resolve_targets(b.targets)
            --b:addHost("konghq.com", 8000, 50)
            local ip, port, hostname, handle = assert(b:getPeer(nil, nil, "xxx"))
            assert.equal("1.2.3.4", ip)
            assert.equal(2, port)
            assert.equal("konghq.com", hostname)
            assert.not_nil(handle)
          end)


          it("returns expected results/types when using SRV with name ('useSRVname=true')", function()
            dnsA({
            { name = "getkong.org", address = "1.2.3.4" },
            })
            dnsSRV({
            { name = "konghq.com", target = "getkong.org", port = 2, weight = 3 },
            })
            local b = new_balancer()
            b.useSRVname = true -- override setting specified when creating

            add_target(b, { name = "konghq.com", port = 8000, weight = 50 })
            targets.resolve_targets(b.targets)
            --b:addHost("konghq.com", 8000, 50)
            local ip, port, hostname, handle = assert(b:getPeer(nil, nil, "xxx"))
            assert.equal("1.2.3.4", ip)
            assert.equal(2, port)
            assert.equal("getkong.org", hostname)
            assert.not_nil(handle)
          end)


          it("returns expected results/types when using A", function()
            dnsA({
              { name = "getkong.org", address = "1.2.3.4" },
            })
            local b = new_balancer()
            targets.resolve_targets(b.targets)
            --b:addHost("getkong.org", 8000, 50)
            local ip, port, hostname, handle = assert(b:getPeer(nil, nil, "xxx"))
            assert.equal("1.2.3.4", ip)
            assert.equal(8000, port)
            assert.equal("getkong.org", hostname)
            assert.not_nil(handle)
          end)


          it("returns expected results/types when using IPv4", function()
            b:addHost("4.3.2.1", 8000, 50)
            local ip, port, hostname, handle = b:getPeer()
            assert.equal("4.3.2.1", ip)
            assert.equal(8000, port)
            assert.equal(nil, hostname)
            assert.equal("userdata", type(handle.__udata))
          end)


          it("returns expected results/types when using IPv6", function()
            b:addHost("::1", 8000, 50)
            local ip, port, hostname, handle = b:getPeer()
            assert.equal("[::1]", ip)
            assert.equal(8000, port)
            assert.equal(nil, hostname)
            assert.equal("userdata", type(handle.__udata))
          end)


          it("fails when there are no addresses added", function()
            assert.same({
              nil, "Balancer is unhealthy", nil, nil,
            }, {
              b:getPeer()
            }
            )
          end)


          it("fails when all addresses are unhealthy", function()
            b:addHost("127.0.0.1", 8000, 100)
            b:addHost("127.0.0.2", 8000, 100)
            b:addHost("127.0.0.3", 8000, 100)
            b:setAddressStatus(false, "127.0.0.1", 8000)
            b:setAddressStatus(false, "127.0.0.2", 8000)
            b:setAddressStatus(false, "127.0.0.3", 8000)
            assert.same({
              nil, "Balancer is unhealthy", nil, nil,
            }, {
              b:getPeer()
            }
            )
          end)


          it("fails when balancer switches to unhealthy", function()
            b:addHost("127.0.0.1", 8000, 100)
            b:addHost("127.0.0.2", 8000, 100)
            b:addHost("127.0.0.3", 8000, 100)
            assert.not_nil(b:getPeer())

            b:setAddressStatus(false, "127.0.0.1", 8000)
            b:setAddressStatus(false, "127.0.0.2", 8000)
            assert.same({
              nil, "Balancer is unhealthy", nil, nil,
            }, {
              b:getPeer()
            }
            )
          end)


          it("recovers when balancer switches to healthy", function()
            b:addHost("127.0.0.1", 8000, 100)
            b:addHost("127.0.0.2", 8000, 100)
            b:addHost("127.0.0.3", 8000, 100)
            assert.not_nil(b:getPeer())

            b:setAddressStatus(false, "127.0.0.1", 8000)
            b:setAddressStatus(false, "127.0.0.2", 8000)
            assert.same({
              nil, "Balancer is unhealthy", nil, nil,
            }, {
              b:getPeer()
            }
            )

            b:setAddressStatus(true, "127.0.0.2", 8000)
            assert.not_nil(b:getPeer())
          end)


          it("recovers when dns entries are replaced by healthy ones", function()
            local record = dnsA({
              { name = "getkong.org", address = "1.2.3.4" },
            })
            b:addHost("getkong.org", 8000, 50)
            assert.not_nil(b:getPeer())

            -- mark it as unhealthy
            assert(b:setAddressStatus(false, "1.2.3.4", 8000, "getkong.org"))
            assert.same({
              nil, "Balancer is unhealthy", nil, nil,
            }, {
              b:getPeer()
            }
            )

            -- expire DNS and add a new backend IP
            -- balancer should now recover since a new healthy backend is available
            record.expire = 0
            dnsA({
              { name = "getkong.org", address = "5.6.7.8" },
            })

            local timeout = ngx.now() + 5   -- we'll try for 5 seconds
            while true do
              assert(ngx.now() < timeout, "timeout")

              local ip = b:getPeer()
              if ip == "5.6.7.8" then
                break  -- expected result, success!
              end

              ngx.sleep(0.1)  -- wait a bit before retrying
            end

          end)

        end)

      end)
    end
  end)
end
