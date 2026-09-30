with lines as (
    select
        id,
        host,
        latency_ms,
        k,
        value
    from app_logs
    lateral view explode(context) t as k, value
)
select
    id,
    latency_ms,
    k,
    value,
    round(latency_ms / sum(latency_ms) over (partition by k), 4) as latency_ms_share,
    ntile(10) over (partition by host order by latency_ms, id, k) as latency_ms_bucket,
    avg(latency_ms) over (order by latency_ms, id, k rows between 3 preceding and current row) as latency_ms_avg_3,
    min(latency_ms) over (partition by host) as host_min_latency_ms,
    last_value(latency_ms, true) over (partition by k, host order by latency_ms, id, k rows between unbounded preceding and current row) as last_known_latency_ms
from lines;
