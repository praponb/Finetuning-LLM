with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        value
    from app_logs
    lateral view outer explode(context) x as k, value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, initcap(trim(service)) as service_title
    from exploded
    where k is not null
      and k is distinct from 'trace'
),
enriched as (
    -- Window metrics over several different partitions
    select
        host,
        latency_ms,
        value,
        percent_rank() over (partition by level order by latency_ms nulls last) as latency_ms_percent_rank,
        round(latency_ms / sum(latency_ms) over (partition by host, level), 4) as latency_ms_share,
        stddev_samp(latency_ms) over (partition by k) as k_stddev_samp_latency_ms,
        round(latency_ms / sum(latency_ms) over (partition by service, host), 4) as latency_ms_share_2
    from cleaned
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        host,
        latency_ms,
        latency_ms_percent_rank,
        latency_ms_share,
        k_stddev_samp_latency_ms,
        latency_ms_share_2,
        least(k_stddev_samp_latency_ms, latency_ms_share) as least_k_stddev_samp_latency_ms_latency_ms_share
    from enriched
    where latency_ms_percent_rank <= 0.51
)
-- Aggregate with conditional metrics
select
    case when latency_ms_share >= 0.69 then 'HIGH' else 'LOW' end as latency_ms_share_tier,
    host,
    count(distinct host) as distinct_hosts,
    size(collect_set(host)) as num_hosts,
    sort_array(collect_set(host)) as host_list
from valid
group by rollup (case when latency_ms_share >= 0.69 then 'HIGH' else 'LOW' end, host)
having size(collect_set(host)) > 1
order by num_hosts desc;
