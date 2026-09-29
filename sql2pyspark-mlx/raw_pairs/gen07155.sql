with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view outer explode(context) t as k, value
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by k order by status_code desc) as status_code_dense_rank,
        cume_dist() over (order by status_code) as status_code_cume_dist,
        avg(latency_ms) over (partition by level) as level_avg_latency_ms,
        dense_rank() over (partition by service order by latency_ms desc nulls first) as latency_ms_dense_rank,
        percent_rank() over (partition by level order by latency_ms) as latency_ms_percent_rank
    from lines
    where latency_ms <= 2002
),
base as (
    -- Basic cleansing and defaults
    select *, latency_ms_percent_rank + latency_ms as latency_ms_percent_rank_plus_latency_ms
    from with_windows
    where level <> 'WARN'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        status_code + status_code_dense_rank as status_code_plus_status_code_dense_rank,
        round(status_code / latency_ms, 2) as status_code_per_latency_ms
    from base
    where k is not null
      and status_code_cume_dist < 0.56
)
-- Native SQL UNPIVOT (wide to long)
select
    host,
    metric_name,
    metric_value
from (
    select
        host,
        status_code_cume_dist,
        latency_ms_percent_rank,
        latency_ms_percent_rank_plus_latency_ms
    from filtered
) src
unpivot (
    metric_value for metric_name in (status_code_cume_dist, latency_ms_percent_rank, latency_ms_percent_rank_plus_latency_ms)
)
