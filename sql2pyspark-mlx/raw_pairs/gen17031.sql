with lines as (
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
flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        attr_key,
        explode_outer(coupon_codes) as value
    from lines
),
filtered as (
    -- Keep recent, valid rows
    select *, case when attr_key in ('utm_source', 'device') then 'Tier 1' else 'Other' end as attr_key_group
    from flattened
    where created_at > date '2025-03-02'
      and attr_key is not null
),
base as (
    -- Basic cleansing and defaults
    select *, upper(value) as value_upper
    from filtered
    where value is not null
),
base_carts as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        created_at,
        attr_key,
        value,
        coalesce(value, 'unassigned') as value_filled,
        coalesce(value, 'NONE') as value_filled_2
    from base
    where created_at < '2023-08-01'
)
select
    id,
    created_at,
    value,
    value_filled,
    value_filled_2
from base_carts
