with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) li as attr_key, value
),
unpacked as (
    -- Semi-structured parsing
    select
        id,
        log_ts,
        status_code,
        attr_key,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 2) != 0 as int) as flag_bit2
    from flattened
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        log_ts,
        status_code,
        attr_key,
        status_code_high,
        datediff(current_date(), log_ts) / 7 as weeks_since_log_ts
    from unpacked
    where log_ts >= date '2025-06-01'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by attr_key order by status_code_high asc, id asc, attr_key asc) as dedup_rank
        from valid
    ) x
    where dedup_rank = 1
)
-- Aggregate with conditional metrics
select
    attr_key,
    case when status_code_high >= 2 then 'PREMIUM' else 'STANDARD' end as status_code_high_tier,
    count(case when attr_key = 'region' then 1 end) as region_count,
    percentile_approx(status_code, 0.95) as status_code_p95,
    sum(case when attr_key = 'version' then weeks_since_log_ts else 0 end) as version_weeks_since_log_ts,
    min(status_code) as min_status_code,
    count_if(attr_key = 'region') as region_events,
    sort_array(collect_set(attr_key)) as attr_key_list
from deduped
group by 1, 2
having min_status_code < 1000000
