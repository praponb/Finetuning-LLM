with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) t as k, attr_value
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        case when k = 'version' then 1 else 0 end as is_version,
        case when k in ('region', 'version') then 'Tier 1' else 'Other' end as k_group
    from exploded
    where k in ('trace', 'version')
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (partition by k order by is_version asc nulls first) as is_version_cume_dist,
        cume_dist() over (partition by k order by is_version desc) as is_version_cume_dist_2,
        cume_dist() over (partition by k order by is_version asc nulls first) as is_version_cume_dist_3,
        sum(is_version) over (partition by k) as k_sum_is_version
    from cleaned
    where is_version = 50
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        k,
        attr_value,
        k_group,
        is_version_cume_dist_2,
        k_sum_is_version,
        substring(k, 1, 2) as k_prefix,
        coalesce(k, 'DIRECT') as k_filled
    from scored
    where k is not null
)
-- Multi-level aggregation
select
    case when is_version_cume_dist_2 >= 0.64 then 'PREMIUM' else 'STANDARD' end as is_version_cume_dist_2_tier,
    k,
    approx_percentile(k_sum_is_version, array(0.25, 0.5, 0.75)) as k_sum_is_version_quartiles,
    count(*) filter (where k = 'region') as region_rows,
    grouping(case when is_version_cume_dist_2 >= 0.64 then 'PREMIUM' else 'STANDARD' end) as grouping_id
from base
group by grouping sets ((case when is_version_cume_dist_2 >= 0.64 then 'PREMIUM' else 'STANDARD' end, k), (case when is_version_cume_dist_2 >= 0.64 then 'PREMIUM' else 'STANDARD' end), ());
