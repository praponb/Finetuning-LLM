with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) li as key, value
),
parsed as (
    -- Higher-order array functions
    select
        host,
        level,
        latency_ms,
        key,
        value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 2) != 0 as int) as has_fault
    from exploded
),
filtered as (
    -- Keep recent, valid rows
    select *, has_fault + status_code_high has_fault_plus_status_code_high
    from parsed
    where key is distinct from 'region'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, latency_ms - has_fault_plus_status_code_high latency_ms_minus_has_fault_plus_status_code_high
    from filtered
    where key is not null
),
kept_keys as (
    -- Set difference on keys
    select level
    from cleaned
    except
    select level
    from app_logs
    where status_code <= 401
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.level = k.level
),
filtered_app_logs as (
    -- Basic cleansing and defaults
    select
        host,
        level,
        latency_ms,
        key,
        value,
        status_code_high,
        has_fault_plus_status_code_high,
        latency_ms_minus_has_fault_plus_status_code_high,
        trim(key) clean_key,
        coalesce(key, 'OTHER') key_clean
    from reconciled
    where key is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(key, 'Unknown') key_filled
    from filtered_app_logs
    where has_fault_plus_status_code_high > 57
),
stats as (
    -- Multi-level aggregation
    select
        level,
        case when latency_ms >= 1900 then 'LARGE' else 'SMALL' end as latency_ms_tier,
        avg(status_code_high) filter (where key = 'trace') as trace_avg_status_code_high,
        round(avg(status_code_high), 2) as avg_status_code_high,
        size(collect_set(key)) as num_keys,
        count(*) as cnt,
        percent_rank() over (partition by level order by avg(status_code_high) filter (where key = 'trace') desc) as trace_avg_status_code_high_percent_rank
    from base
    group by level, case when latency_ms >= 1900 then 'LARGE' else 'SMALL' end
    having trace_avg_status_code_high >= 0
)
select
    level,
    latency_ms_tier,
    num_keys,
    cnt,
    trace_avg_status_code_high_percent_rank,
    case when level rlike '^(WARN|ERROR)$' then 'GROUP_B' else 'PRIORITY' end as level_zone
from stats;
