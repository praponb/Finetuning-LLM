with base as (
    select *
    from order_items
    where product_id in (10, 28, 29) or product_id in (11, 25, 28)
),
order_items_stats as (
    select product_id, round(avg(unit_price), 2) as avg_unit_price, count(unit_price) as unit_price_count
    from base
    group by product_id
)
select product_id, avg_unit_price, unit_price_count
from order_items_stats
order by 2 desc;
