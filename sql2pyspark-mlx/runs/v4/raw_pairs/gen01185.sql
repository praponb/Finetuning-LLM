with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        v
    from app_logs
    lateral view outer explode(context) e as k, v
),
remaining_keys as (
    -- Set intersection on keys
    select service, host
    from exploded
    intersect
    select service, host
    from app_logs
    where host in ('h1', 'h4', 'h3')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    inner join remaining_keys k on s.service = k.service and s.host = k.host
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        v,
        sum(latency_ms) over (partition by k order by cast(log_ts as bigint) range between 1800 preceding and current row) as latency_ms_sum_30m,
        round(latency_ms / sum(latency_ms) over (partition by host), 4) as latency_ms_share
    from verified
)
-- Aggregate with conditional metrics
select
    case when latency_ms_share >= 0.77 then 'LARGE' else 'SMALL' end as latency_ms_share_tier,
    count(case when service = 'auth' then 1 end) as auth_count,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as duration_min
from enriched
group by latency_ms_share_tier
having count(case when service = 'auth' then 1 end) > 1
order by auth_count desc nulls first;
