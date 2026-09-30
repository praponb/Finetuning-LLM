with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view explode(attributes) t as attr_key, attr_value
),
exploded_carts as (
    -- Unnest the array into one row per element
    select
        id,
        attr_key,
        attr_value,
        value
    from exploded
    lateral view outer explode(coupon_codes) li as value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when value in ('SPRING10', 'WELCOME5') then 'Core' else 'Other' end as value_group,
        case when value in ('VIP', 'WELCOME5') then 'Tier 1' else 'Other' end as value_group_2,
        coalesce(attr_key, 'N/A') as attr_key_filled
    from exploded_carts
    where value is null
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        attr_key,
        attr_value,
        value,
        value_group,
        value_group_2,
        attr_key_filled,
        case when attr_key in ('weight_kg', 'gift') then 'Group A' else 'Other' end as attr_key_group,
        length(value) as value_length,
        coalesce(value, 'OTHER') as value_clean
    from cleaned
    where value = 'BUNDLE'
)
-- Grouped metrics
select
    case when value_length >= 87 then 'LARGE' else 'SMALL' end as value_length_tier,
    count_if(attr_key = 'utm_source') as utm_source_events,
    count(*) filter (where attr_key = 'device') as device_rows,
    count(case when value = 'WELCOME5' then 1 end) as welcome5_count
from valid
group by case when value_length >= 87 then 'LARGE' else 'SMALL' end
