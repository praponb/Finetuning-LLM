with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        key,
        value
    from app_logs
    lateral view explode(context) t as key, value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(current_date(), log_ts) / 365 as years_since_log_ts,
        case when service = 'search' then 1 else 0 end as is_search,
        coalesce(key, 'UNKNOWN') as key_filled
    from exploded
    where log_ts >= to_date('2025-06-01')
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from filtered
    qualify rank() over (partition by key, host order by years_since_log_ts desc) = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from deduped
    qualify rank() over (partition by service, key order by years_since_log_ts desc nulls last) = 1
)
select
    id,
    service,
    host,
    key,
    value,
    years_since_log_ts,
    case when service rlike '^(checkout|billing)$' then 'PRIORITY' when service rlike '^(auth|search)$' then 'CORE' else 'GROUP_B' end as service_zone
from latest
order by key, years_since_log_ts
