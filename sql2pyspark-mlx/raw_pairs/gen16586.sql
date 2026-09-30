with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) x as key, attr_value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        host,
        level,
        status_code,
        key,
        attr_value,
        cast(status_code as string) as status_code_str,
        trim(host) as clean_host,
        coalesce(key, 'N/A') as key_clean
    from exploded
    where log_ts > date '2025-06-01'
      and status_code between 379 and 433
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        level,
        status_code,
        clean_host,
        round(status_code / sum(status_code) over (partition by level), 4) as status_code_share,
        cume_dist() over (partition by key order by status_code desc) as status_code_cume_dist
    from cleaned
),
enriched as (
    -- Ranking and gap features
    select *, count(status_code_cume_dist) over (partition by level) as level_count_status_code_cume_dist
    from with_windows
    where status_code_cume_dist <= 0.5
),
with_windows_app_logs as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (order by status_code desc) as status_code_rank,
        cume_dist() over (partition by level order by status_code_cume_dist desc nulls last) as status_code_cume_dist_cume_dist
    from enriched
)
-- Multi-level aggregation
select
    case when status_code_cume_dist_cume_dist >= 0.61 then 'LARGE' else 'SMALL' end as status_code_cume_dist_cume_dist_tier,
    level,
    count(distinct level) as distinct_levels,
    size(collect_set(level)) as num_levels,
    count(case when level = 'ERROR' then 1 end) as error_count,
    count(distinct id) as distinct_ids,
    size(collect_set(level)) as num_levels_2,
    count(*) filter (where level = 'DEBUG') as debug_rows
from with_windows_app_logs
group by case when status_code_cume_dist_cume_dist >= 0.61 then 'LARGE' else 'SMALL' end, level
