with flattened as (
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    from app_logs
    lateral view explode(context) x as key, value
)
select
    id,
    host,
    key,
    min(latency_ms) over (partition by host order by latency_ms, id, key rows between 6 preceding and current row) as last7_min_latency_ms,
    avg(latency_ms) over (partition by host order by latency_ms, id, key rows between 6 preceding and current row) as last7_avg_latency_ms,
    count(latency_ms) over (partition by host order by latency_ms, id, key rows between 6 preceding and current row) as last7_count_latency_ms,
    sum(latency_ms) over (partition by host order by latency_ms, id, key rows between 6 preceding and current row) as last7_sum_latency_ms
from flattened
where host = 'h4'
