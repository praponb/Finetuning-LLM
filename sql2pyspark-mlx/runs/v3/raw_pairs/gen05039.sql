with filtered_order_items as (
    select *
    from order_items
    where unit_price < 310 and unit_price is not null
),
ProductIdSummary as (
    select product_id, round(stddev(unit_price), 2) as stddev_unit_price
    from filtered_order_items
    group by product_id
)
select product_id, stddev_unit_price
from ProductIdSummary
