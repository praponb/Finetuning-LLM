with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        k,
        v
    from app_logs
    lateral view explode(context) e as k, v
),
matched_keys as (
    -- Set difference on keys
    select id, level
    from exploded
    except
    select id, level
    from app_logs
    where level not in ('INFO', 'DEBUG')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    inner join matched_keys k on s.id = k.id and s.level = k.level
),
kept_keys as (
    -- Set difference on keys
    select host
    from reconciled
    except all
    select host
    from app_logs
    where status_code < 330
),
reconciled_app_logs as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled s
    inner join kept_keys k on s.host = k.host
),
scored as (
    -- Running and moving aggregates
    select
        *,
        avg(latency_ms) over (partition by k, level) as k_avg_latency_ms,
        round(latency_ms / sum(latency_ms) over (partition by level, host), 4) as latency_ms_share,
        avg(latency_ms) over (partition by level) as level_avg_latency_ms
    from reconciled_app_logs
    where latency_ms <> 2060
)
select
    host,
    latency_ms,
    k,
    v,
    latency_ms_share
from scored
order by latency_ms desc nulls first, latency_ms_share desc
