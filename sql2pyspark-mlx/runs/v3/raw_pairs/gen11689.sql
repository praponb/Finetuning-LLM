with valid_order_items as (
    select *
    from order_items
    where order_id in (3, 13, 32) and unit_price between 280 and 390
),
order_items_stats as (
    select product_id, max(unit_price) - min(unit_price) as unit_price_range
    from valid_order_items
    group by product_id
)
select product_id, unit_price_range
from order_items_stats
where unit_price_range >= 54;
