with order_totals as (
    select order_id, max(quantity) as max_quantity, max(unit_price) - min(unit_price) as unit_price_range
    from order_items
    where product_id in (7, 26, 30)
    group by order_id
)
select
    o.id,
    o.store_id,
    o.order_date,
    ot.max_quantity,
    ot.unit_price_range
from orders o
inner join order_totals ot on o.id = ot.order_id;
