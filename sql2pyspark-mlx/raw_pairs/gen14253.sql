with exploded as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    from app_logs
    lateral view explode(context) e as key, attr_value
)
select
    id,
    host,
    level,
    latency_ms,
    key,
    sum(latency_ms) over w_hist as running_sum_latency_ms,
    min(latency_ms) over w_hist as running_min_latency_ms,
    count(latency_ms) over w_hist as running_count_latency_ms
from exploded
window w_hist as (partition by level order by log_ts, id, key rows between unbounded preceding and current row);
