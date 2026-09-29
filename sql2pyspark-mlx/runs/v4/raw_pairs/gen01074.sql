with src as (
    select *, coalesce(quantity, 0) as quantity_filled
    from order_items
    where product_id is not null and (product_id in (7, 8, 30, 31) or quantity <= 5)
),
product_id_summary as (
    select product_id, count(quantity_filled) as quantity_filled_count, round(stddev(quantity), 2) as stddev_quantity
    from src
    group by product_id
)
select product_id, stddev_quantity
from product_id_summary
order by stddev_quantity desc;
