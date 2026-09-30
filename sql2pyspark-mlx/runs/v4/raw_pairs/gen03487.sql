with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) x as key, attr_value
),
unpacked as (
    -- Semi-structured parsing
    select
        service,
        level,
        key,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as is_flagged
    from exploded
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        service,
        level,
        key,
        status_code_high,
        is_flagged,
        substring(key, 1, 2) as key_prefix,
        trim(level) as clean_level,
        coalesce(key, 'N/A') as key_clean
    from unpacked
    where key is not null
      and level <> 'DEBUG'
),
scored as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by is_flagged desc) as is_flagged_dense_rank,
        percent_rank() over (partition by service order by is_flagged) as is_flagged_percent_rank,
        count(is_flagged) over (partition by key) as key_count_is_flagged
    from filtered
),
matched_keys as (
    -- Set difference on keys
    select service, level
    from scored
    except all
    select service, level
    from app_logs
    where level = 'DEBUG'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join matched_keys k on s.service = k.service and s.level = k.level
),
scored_app_logs as (
    -- Window metrics over several different partitions
    select
        *,
        max(is_flagged) over (partition by level, key) as level_max_is_flagged,
        max(is_flagged_percent_rank) over (partition by level, key) as level_max_is_flagged_percent_rank,
        rank() over (partition by level, key order by is_flagged) as is_flagged_rank
    from reconciled
),
grouped as (
    -- Multi-level aggregation
    select
        case when level_max_is_flagged >= 1 then 'HIGH' else 'LOW' end as level_max_is_flagged_tier,
        case when level_max_is_flagged_percent_rank >= 1 then 'LARGE' else 'SMALL' end as level_max_is_flagged_percent_rank_tier,
        service,
        count_if(key = 'trace') as trace_events,
        approx_percentile(is_flagged_dense_rank, array(0.25, 0.5, 0.75)) as is_flagged_dense_rank_quartiles,
        approx_percentile(is_flagged, array(0.25, 0.5, 0.75)) as is_flagged_quartiles
    from scored_app_logs
    group by case when level_max_is_flagged >= 1 then 'HIGH' else 'LOW' end, case when level_max_is_flagged_percent_rank >= 1 then 'LARGE' else 'SMALL' end, service
)
select
    level_max_is_flagged_tier,
    level_max_is_flagged_percent_rank_tier,
    service,
    trace_events,
    is_flagged_dense_rank_quartiles,
    is_flagged_quartiles
from grouped;
