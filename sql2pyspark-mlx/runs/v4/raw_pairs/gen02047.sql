with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity - unit_price as line_total
    from order_items
    where quantity > 6
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        unit_price,
        line_total,
        round(quantity * 0.9, 2) as quantity_adjusted,
        unit_price - line_total as unit_price_minus_line_total
    from base
    where product_id is not null
      and product_id in (1, 15, 25, 29)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from filtered
    qualify row_number() over (partition by product_id order by unit_price_minus_line_total, id) = 1
)
select
    id,
    product_id,
    line_total,
    quantity_adjusted,
    unit_price_minus_line_total
from latest
order by line_total desc
