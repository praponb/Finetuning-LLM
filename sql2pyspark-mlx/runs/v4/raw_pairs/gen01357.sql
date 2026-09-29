with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        v
    from app_logs
    lateral view outer explode(context) li as key, v
),
flattened_attrs as (
    -- Semi-structured parsing
    select
        service,
        log_ts,
        key,
        cast((status_code & 2) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from flattened
    where service = 'search'
),
base as (
    -- Basic cleansing and defaults
    select
        service,
        log_ts,
        is_flagged,
        to_date(log_ts) as log_ts_date,
        substring(service, 1, 2) as service_prefix,
        coalesce(key, 'UNKNOWN') as key_filled
    from flattened_attrs
    where is_flagged < 1
)
-- Grouped metrics
select
    service,
    min(log_ts) as first_seen,
    sum(is_flagged) filter (where service = 'checkout') as checkout_total_is_flagged,
    sum(case when service = 'billing' then is_flagged else 0 end) as billing_is_flagged
from base
group by service
having sum(is_flagged) filter (where service = 'checkout') >= 0
order by checkout_total_is_flagged asc nulls first
