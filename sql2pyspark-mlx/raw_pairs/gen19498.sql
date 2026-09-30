with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view outer explode(attributes) t as key, attr_value
),
flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        explode_outer(coupon_codes) as code
    from exploded
),
filtered as (
    -- Keep recent, valid rows
    select *, date_trunc('MONTH', created_at) as created_at_period
    from flattened
    where code is not null
),
filtered_carts as (
    -- Basic cleansing and defaults
    select
        *,
        length(code) as code_length,
        coalesce(code, 'DIRECT') as resolved_code
    from filtered
    where created_at >= to_date('2025-03-01')
      and code is not null
),
base as (
    -- Keep recent, valid rows
    select
        created_at,
        code,
        created_at_period,
        code_length,
        resolved_code,
        floor(code_length) as code_length_whole
    from filtered_carts
    where code is not null
      and code is distinct from 'VIP'
)
select
    created_at,
    code,
    created_at_period,
    code_length,
    resolved_code,
    code_length_whole,
    case when code rlike '^(WELCOME5|VIP)$' then 'EXTENDED' when code rlike '^(SPRING10|BUNDLE)$' then 'GROUP_A' else 'CORE' end as code_zone
from base
order by code_length_whole desc;
