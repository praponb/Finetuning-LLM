with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        latency_ms,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) t as attr_key, v
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        stddev_samp(latency_ms) over (partition by host) as host_stddev_samp_latency_ms,
        stddev_samp(latency_ms) over (partition by host, attr_key) as host_stddev_samp_latency_ms_2
    from flattened
    where host = 'h2'
),
scored_app_logs as (
    -- Running and moving aggregates
    select *, round(host_stddev_samp_latency_ms / sum(host_stddev_samp_latency_ms) over (partition by level, host), 4) as host_stddev_samp_latency_ms_share
    from scored
),
cleaned as (
    -- Keep recent, valid rows
    select
        service,
        host,
        level,
        latency_ms,
        attr_key,
        length(level) as level_length,
        host_stddev_samp_latency_ms + host_stddev_samp_latency_ms_share as host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share,
        coalesce(attr_key, 'OTHER') as resolved_attr_key
    from scored_app_logs
    where attr_key is not null
      and service in ('auth', 'checkout', 'search')
),
cleaned_app_logs as (
    -- Filter the raw rows and derive helper columns
    select *, case when attr_key = 'region' then 1 else 0 end as is_region
    from cleaned
    where attr_key is not null
      and attr_key is distinct from 'trace'
),
cleaned_app_logs_2 as (
    -- Keep recent, valid rows
    select
        *,
        host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share * is_region as host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region,
        coalesce(attr_key, 'N/A') as resolved_attr_key_2
    from cleaned_app_logs
    where latency_ms between 749 and 1759
),
metrics as (
    -- Ranking and gap features
    select
        level,
        resolved_attr_key,
        host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region,
        dense_rank() over (partition by host order by latency_ms) as latency_ms_dense_rank
    from cleaned_app_logs_2
),
grouped as (
    -- Grouped metrics
    select
        case when latency_ms_dense_rank >= 9 then 'LARGE' else 'SMALL' end as latency_ms_dense_rank_tier,
        avg(latency_ms_dense_rank) as avg_latency_ms_dense_rank,
        sort_array(collect_set(level)) as level_list,
        max(host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region) filter (where level = 'DEBUG') as debug_max_host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region,
        sort_array(collect_set(level)) as level_list_2,
        count(case when level = 'WARN' then 1 end) as warn_count
    from metrics
    group by latency_ms_dense_rank_tier
    having debug_max_host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region < 1000000
)
select
    latency_ms_dense_rank_tier,
    avg_latency_ms_dense_rank,
    level_list,
    debug_max_host_stddev_samp_latency_ms_plus_host_stddev_samp_latency_ms_share_times_is_region,
    level_list_2,
    warn_count
from grouped;
