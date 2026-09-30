with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when host = 'h3' then 1 else 0 end as is_h3,
        coalesce(attr_key, 'N/A') as attr_key_filled
    from flattened
    where attr_key = 'version'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by attr_key, host order by is_h3 desc) as is_h3_dense_rank,
        min(is_h3) over (partition by host, service) as host_min_is_h3,
        dense_rank() over (partition by level, service order by is_h3 desc nulls first) as is_h3_dense_rank_2,
        stddev_samp(is_h3) over (partition by host) as host_stddev_samp_is_h3,
        cume_dist() over (partition by host, service order by is_h3 desc nulls last) as is_h3_cume_dist
    from valid
    where service in ('auth', 'checkout', 'search')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(is_h3_dense_rank_2, 0) as is_h3_dense_rank_2_filled
    from enriched
    where attr_key is not null
      and is_h3_dense_rank_2 < 5
)
-- Window metrics over several different partitions
select
    id,
    service,
    level,
    attr_key,
    v,
    attr_key_filled,
    host_min_is_h3,
    host_stddev_samp_is_h3,
    percent_rank() over (order by host_min_is_h3 desc) as host_min_is_h3_percent_rank,
    stddev_samp(is_h3_dense_rank_2_filled) over (partition by host) as host_stddev_samp_is_h3_dense_rank_2_filled
from cleaned;
