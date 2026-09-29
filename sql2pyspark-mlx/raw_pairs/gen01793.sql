with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
),
unpacked as (
    -- Higher-order array functions
    select
        id,
        attr_value,
        cast((status_code & 8) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from lines
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (order by status_code_high nulls last) as status_code_high_percent_rank,
        rank() over (order by has_fault) as has_fault_rank,
        rank() over (order by status_code_high nulls last) as status_code_high_rank,
        percent_rank() over (order by has_fault desc nulls last) as has_fault_percent_rank,
        rank() over (order by status_code_high desc) as status_code_high_rank_2
    from unpacked
    where status_code_high is null
),
matched_keys as (
    -- Set intersection on keys
    select id
    from with_windows
    intersect
    select id
    from app_logs
    where host = 'h3'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    join matched_keys k on s.id = k.id
),
cleaned as (
    -- Basic cleansing and defaults
    select *, greatest(status_code_high_rank, has_fault) as greatest_status_code_high_rank_has_fault
    from kept_rows
    where has_fault > 1
)
-- Aggregate with conditional metrics
select
    case when has_fault_rank >= 8 then 'HIGH' else 'LOW' end as has_fault_rank_tier,
    case when status_code_high_rank >= 7 then 'PREMIUM' else 'STANDARD' end as status_code_high_rank_tier,
    sum(status_code_high_rank) as total_status_code_high_rank,
    approx_percentile(has_fault_rank, 0.95) as has_fault_rank_p95,
    count(distinct id) as distinct_ids,
    sum(status_code_high) as total_status_code_high,
    count(distinct id) as distinct_ids_2
from cleaned
group by 1, 2
order by total_status_code_high desc
