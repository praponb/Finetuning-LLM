with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view explode(attributes) e as key, value
),
lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        key,
        explode_outer(coupon_codes) as element
    from flattened
),
base as (
    -- Basic cleansing and defaults
    select *, concat(key, ', ', element) as key_element
    from lines
    where created_at >= timestamp '2025-03-01 21:00:00'
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        key,
        datediff(current_date(), created_at) / 7 as weeks_since_created_at
    from base
    where created_at >= to_date('2025-03-01')
      and element is not null
)
select key
from cleaned;
