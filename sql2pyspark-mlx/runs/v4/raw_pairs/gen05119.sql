with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view explode(context) x as key, attr_value
),
parsed as (
    -- Higher-order array functions
    select
        id,
        level,
        key,
        cast((status_code & 2) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from lines
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from parsed
    qualify row_number() over (partition by level, key order by has_fault desc, id asc, key asc) = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from deduped
    qualify row_number() over (partition by key order by has_fault asc, id asc, key asc) = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        level,
        key,
        has_fault,
        status_code_high,
        sum(has_fault) over (partition by key order by has_fault asc, id asc, key asc rows between unbounded preceding and current row) as cumulative_has_fault,
        lag(status_code_high, 1) over (order by has_fault asc, id asc, key asc) as prev_status_code_high
    from latest
),
enriched_app_logs as (
    -- Window metrics over several different partitions
    select
        id,
        level,
        key,
        has_fault,
        status_code_high,
        cumulative_has_fault,
        prev_status_code_high,
        sum(status_code_high) over (partition by level order by prev_status_code_high asc, id asc, key asc rows between unbounded preceding and current row) as running_status_code_high,
        sum(has_fault) over (partition by level order by cumulative_has_fault asc, id asc, key asc rows between unbounded preceding and current row) as running_has_fault
    from enriched
)
select
    id,
    key,
    has_fault,
    status_code_high,
    cumulative_has_fault,
    running_has_fault
from enriched_app_logs
order by status_code_high desc nulls first;
