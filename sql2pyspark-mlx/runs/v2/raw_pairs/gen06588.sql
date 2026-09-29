with order_totals as (
    select order_id, max(unit_price) as max_unit_price, max(quantity) - min(quantity) as quantity_range
    from order_items
    where order_id in (5, 29, 38, 40)
    group by order_id
)
select
    o.id,
    o.customer_id,
    o.discount,
    ot.max_unit_price,
    ot.quantity_range
from orders o
join order_totals ot on o.id = ot.order_id
order by customer_id, id
