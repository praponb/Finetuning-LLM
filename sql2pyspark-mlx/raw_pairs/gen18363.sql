with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        value
    from carts
    lateral view outer explode(coupon_codes) x as value
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        initcap(trim(value)) as value_title,
        coalesce(value, 'Unknown') as value_filled
    from flattened
    where value is not null
),
matched_keys as (
    -- Set intersection on keys
    select id
    from cleaned
    intersect
    select id
    from carts
    where array_contains(coupon_codes, 'SPRING10')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.id = k.id
),
base as (
    -- Basic cleansing and defaults
    select *, date_format(created_at, 'yyyy-MM-dd') as created_at_label
    from kept_rows
    where created_at >= to_timestamp('2025-03-01 14:00:00')
),
daily as (
    -- Multi-level aggregation
    select
        value,
        count(distinct value) as distinct_values,
        size(collect_set(value)) as num_values,
        count(*) as cnt
    from base
    group by 1
)
select
    value,
    num_values,
    cnt
from daily
order by cnt nulls first;
