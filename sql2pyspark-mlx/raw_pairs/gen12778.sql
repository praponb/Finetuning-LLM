with remaining_keys as (
    -- Set difference on keys
    select session_id, id
    from web_events
    except
    select session_id, id
    from web_events
    where event_time < '2025-03-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from web_events s
    join remaining_keys k on s.session_id = k.session_id and s.id = k.id
),
base as (
    -- Keep recent, valid rows
    select
        id,
        duration_sec,
        date_format(event_time, 'yyyy-MM-dd') as event_time_label,
        upper(page) as page_upper
    from verified
    where duration_sec > 359
),
cleaned as (
    -- Keep recent, valid rows
    select *, round(duration_sec * 1.1, 2) as duration_sec_scaled
    from base
    where duration_sec <= 399
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        duration_sec / nullif(duration_sec_scaled, 0) as duration_sec_per_duration_sec_scaled,
        cast(duration_sec as double) as duration_sec_str
    from cleaned
    where duration_sec between 436 and 437
),
valid_web_events as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        duration_sec,
        event_time_label,
        duration_sec_scaled,
        duration_sec_per_duration_sec_scaled,
        duration_sec + duration_sec_scaled as duration_sec_plus_duration_sec_scaled,
        least(duration_sec_scaled, duration_sec) as least_duration_sec_scaled_duration_sec
    from valid
    where duration_sec_str between 35 and 58
),
enriched as (
    -- Running and moving aggregates
    select *, rank() over (order by duration_sec asc) as duration_sec_rank
    from valid_web_events
),
enriched_web_events as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by least_duration_sec_scaled_duration_sec asc nulls first) as least_duration_sec_scaled_duration_sec_dense_rank,
        dense_rank() over (order by duration_sec_plus_duration_sec_scaled desc) as duration_sec_plus_duration_sec_scaled_dense_rank
    from enriched
),
totals as (
    -- Grouped metrics
    select
        case when duration_sec_per_duration_sec_scaled >= 81 then 'LARGE' else 'SMALL' end as duration_sec_per_duration_sec_scaled_tier,
        case when duration_sec_plus_duration_sec_scaled_dense_rank >= 7 then 'LARGE' else 'SMALL' end as duration_sec_plus_duration_sec_scaled_dense_rank_tier,
        case when duration_sec_rank >= 9 then 'HIGH' else 'LOW' end as duration_sec_rank_tier,
        bit_or(duration_sec) as duration_sec_bit_or,
        avg(least_duration_sec_scaled_duration_sec) as avg_least_duration_sec_scaled_duration_sec,
        avg(duration_sec_plus_duration_sec_scaled_dense_rank) as avg_duration_sec_plus_duration_sec_scaled_dense_rank,
        grouping_id() as grouping_id
    from enriched_web_events
    group by grouping sets ((case when duration_sec_per_duration_sec_scaled >= 81 then 'LARGE' else 'SMALL' end, case when duration_sec_plus_duration_sec_scaled_dense_rank >= 7 then 'LARGE' else 'SMALL' end, case when duration_sec_rank >= 9 then 'HIGH' else 'LOW' end), (case when duration_sec_per_duration_sec_scaled >= 81 then 'LARGE' else 'SMALL' end, case when duration_sec_plus_duration_sec_scaled_dense_rank >= 7 then 'LARGE' else 'SMALL' end), (case when duration_sec_per_duration_sec_scaled >= 81 then 'LARGE' else 'SMALL' end))
)
select
    duration_sec_per_duration_sec_scaled_tier,
    duration_sec_bit_or,
    avg_least_duration_sec_scaled_duration_sec,
    avg_duration_sec_plus_duration_sec_scaled_dense_rank,
    grouping_id
from totals
