with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        status_code,
        k,
        v
    from app_logs
    lateral view outer explode(context) x as k, v
),
unpacked as (
    -- Semi-structured parsing
    select
        id,
        status_code,
        k,
        v,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as is_flagged
    from exploded
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        status_code,
        k,
        v,
        status_code_high,
        is_flagged,
        case when k = 'trace' then 1 else 0 end as is_trace,
        floor(is_flagged) as is_flagged_whole
    from unpacked
    where k is not null
      and k is distinct from 'trace'
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        is_flagged_whole * is_flagged as is_flagged_whole_times_is_flagged,
        floor(status_code_high) as status_code_high_whole
    from valid
    where k is not null
      and is_flagged_whole between 44 and 59
)
-- Multi-level aggregation
select
    case when status_code >= 421 then 'HIGH' else 'LOW' end as status_code_tier,
    sum(is_flagged) filter (where k = 'version') as version_total_is_flagged,
    sort_array(collect_set(k)) as k_list,
    count(distinct id) as distinct_ids,
    size(collect_set(k)) as num_ks,
    approx_percentile(status_code_high, array(0.25, 0.5, 0.75)) as status_code_high_quartiles,
    count_if(k = 'version') as version_events
from base
group by case when status_code >= 421 then 'HIGH' else 'LOW' end
order by version_total_is_flagged desc
