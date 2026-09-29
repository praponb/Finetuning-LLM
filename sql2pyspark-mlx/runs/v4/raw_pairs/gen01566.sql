with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) e as k, value
)
select
    date_trunc('day', log_ts) as activity_day,
    host,
    size(collect_set(level)) as num_levels,
    min(log_ts) as first_seen
from flattened
group by date_trunc('day', log_ts), host
order by activity_day desc;
