with exploded as (
    select
        id,
        service,
        host,
        latency_ms,
        key,
        value
    from app_logs
    lateral view outer explode(context) li as key, value
),
cleaned as (
    select
        *,
        round(latency_ms, 0) as latency_ms_rounded,
        concat(host, ' ', key) as host_key
    from exploded
    where service = 'search'
),
remaining_keys as (
    select service, host
    from cleaned
    intersect
    select service, host
    from app_logs
    where host not in ('h1', 'h3')
),
reconciled as (
    select s.*
    from cleaned s
    inner join remaining_keys k on s.service = k.service and s.host = k.host
),
base as (
    select
        *,
        trim(key) as clean_key,
        coalesce(key, 'N/A') as key_clean
    from reconciled
    where key is not null
),
enriched as (
    select
        *,
        stddev_samp(latency_ms) over (partition by host) as host_stddev_samp_latency_ms,
        round(latency_ms / sum(latency_ms) over (partition by host), 4) as latency_ms_share
    from base
),
metrics as (
    select
        *,
        stddev_samp(latency_ms_share) over (partition by host) as host_stddev_samp_latency_ms_share,
        stddev_samp(latency_ms_share) over (partition by key) as key_stddev_samp_latency_ms_share,
        percent_rank() over (partition by key order by latency_ms_share nulls first) as latency_ms_share_percent_rank
    from enriched
),
scored as (
    select
        *,
        count(latency_ms_share) over (partition by key, service) as key_count_latency_ms_share,
        percent_rank() over (partition by service, host order by host_stddev_samp_latency_ms_share) as host_stddev_samp_latency_ms_share_percent_rank,
        dense_rank() over (order by latency_ms_share nulls last) as latency_ms_share_dense_rank
    from metrics
)
select
    service,
    count(case when host = 'h3' then 1 end) as h3_count,
    bit_or(latency_ms) as latency_ms_bit_or
from scored
group by rollup (service)
order by service desc nulls first
