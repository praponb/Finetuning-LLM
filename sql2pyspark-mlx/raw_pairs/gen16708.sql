with lines as (
    select
        id,
        host,
        log_ts,
        latency_ms,
        key,
        attr_value
    from app_logs
    lateral view explode(context) e as key, attr_value
),
latest as (
    select
        id,
        latency_ms,
        key
    from lines
    qualify row_number() over (partition by host order by latency_ms desc, id asc, key asc) = 1
),
metrics as (
    select
        *,
        count(latency_ms) over (partition by key order by latency_ms asc, id asc, key asc rows between 2 preceding and current row) as last3_count_latency_ms,
        min(latency_ms) over (partition by key order by latency_ms asc, id asc, key asc rows between 2 preceding and current row) as last3_min_latency_ms,
        sum(latency_ms) over (partition by key order by latency_ms asc, id asc, key asc rows between 2 preceding and current row) as last3_sum_latency_ms,
        max(latency_ms) over (partition by key order by latency_ms asc, id asc, key asc rows between 2 preceding and current row) as last3_max_latency_ms,
        first_value(key, true) over (partition by key order by latency_ms asc, id asc, key asc rows between unbounded preceding and current row) as first_known_key
    from latest
),
ranked as (
    select
        id,
        latency_ms,
        key,
        last3_count_latency_ms,
        last3_sum_latency_ms,
        first_known_key
    from (
        select *, row_number() over (partition by key order by last3_sum_latency_ms desc, id asc, key asc) as row_num
        from metrics
    ) ranked
    where row_num = 1
)
select
    case when last3_sum_latency_ms >= 10000 then 'LARGE' else 'SMALL' end as last3_sum_latency_ms_tier,
    count(*) filter (where key = 'trace') as trace_rows,
    round(avg(latency_ms), 2) as avg_latency_ms
from ranked
group by case when last3_sum_latency_ms >= 10000 then 'LARGE' else 'SMALL' end
having round(avg(latency_ms), 2) >= 0
