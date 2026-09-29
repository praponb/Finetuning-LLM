with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        quantity,
        unit_price,
        cast(unit_price as int) as unit_price_int,
        round(unit_price, 0) as unit_price_rounded
    from order_items
    where unit_price >= 320
),
matched_keys as (
    -- Set intersection on keys
    select id
    from cleaned
    intersect
    select id
    from order_items
    where unit_price >= 210
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.id = k.id
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        quantity,
        unit_price_int,
        unit_price_rounded,
        unit_price_rounded / nullif(unit_price_int, 0) as unit_price_rounded_per_unit_price_int,
        unit_price_int + quantity as unit_price_int_plus_quantity
    from reconciled
    where unit_price_int <= 25
),
cleaned_order_items as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        quantity,
        unit_price_rounded,
        unit_price_int_plus_quantity,
        cast(unit_price_int as string) as unit_price_int_num
    from valid
    where unit_price_rounded between 25 and 64
)
select
    id,
    quantity,
    unit_price_rounded,
    unit_price_int_plus_quantity,
    unit_price_int_num
from cleaned_order_items
order by unit_price_rounded
