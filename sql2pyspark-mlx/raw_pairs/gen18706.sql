with flattened as (
    select
        id,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) t as attr_key, attr_value
),
scored as (
    select
        *,
        count(latency_ms) over w_grp as running_count_latency_ms,
        avg(latency_ms) over w_grp as running_avg_latency_ms,
        max(latency_ms) over w_grp as running_max_latency_ms,
        min(latency_ms) over w_grp as running_min_latency_ms
    from flattened
    window w_grp as (partition by level, attr_key order by latency_ms, id, attr_key rows between unbounded preceding and current row)
),
valid as (
    select
        id,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value,
        running_max_latency_ms,
        running_min_latency_ms,
        case when running_min_latency_ms >= 1850 then 'High' when running_min_latency_ms >= 960 then 'Medium' else 'Low' end as running_min_latency_ms_tier,
        cast(latency_ms as double) as latency_ms_num
    from scored
    where attr_key is distinct from 'region'
),
latest as (
    select *, row_number() over (partition by host order by latency_ms_num desc nulls last, id, attr_key) as latest_rank
    from valid
    qualify latest_rank = 1
),
kept_keys as (
    select host
    from latest
    intersect all
    select host
    from app_logs
    where level in ('WARN', 'DEBUG', 'ERROR')
),
reconciled as (
    select s.*
    from latest s
    join kept_keys k on s.host = k.host
)
select
    level,
    case when running_max_latency_ms >= 2230 then 'PREMIUM' else 'STANDARD' end as running_max_latency_ms_tier,
    sum(case when attr_key = 'trace' then running_max_latency_ms else 0 end) as trace_running_max_latency_ms,
    count(case when attr_key = 'version' then 1 end) as version_count,
    count(distinct host) as distinct_hosts
from reconciled
group by level, running_max_latency_ms_tier
order by trace_running_max_latency_ms nulls first;
