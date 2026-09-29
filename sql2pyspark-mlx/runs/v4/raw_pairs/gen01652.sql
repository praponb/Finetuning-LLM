with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view explode(attributes) x as attr_key, attr_value
),
lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        attr_key,
        element
    from exploded
    lateral view explode(coupon_codes) t as element
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(attr_key)) as attr_key_title,
        coalesce(attr_key, 'DIRECT') as attr_key_clean
    from lines
    where created_at >= to_date('2025-03-02')
      and attr_key is not null
      and element in ('WELCOME5', 'BUNDLE')
),
base as (
    -- Keep recent, valid rows
    select
        *,
        lower(attr_key) as attr_key_lower,
        coalesce(attr_key, 'N/A') as attr_key_clean_2
    from cleaned
    where created_at > timestamp '2025-03-02 12:00:00'
      and attr_key is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        hour(created_at) as created_at_hour,
        case when attr_key = 'device' then 1 else 0 end as is_device
    from base
    where created_at > to_date('2025-03-02')
      and attr_key is not null
)
select
    created_at,
    attr_key,
    element,
    attr_key_clean,
    attr_key_lower
from filtered
