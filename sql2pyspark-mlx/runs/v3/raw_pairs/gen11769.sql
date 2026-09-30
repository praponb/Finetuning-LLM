with order_stats as (
    select order_id, max(unit_price) as max_unit_price
    from order_items
    where product_id is null
    group by order_id
)
select orders.id, orders.customer_id, order_stats.max_unit_price
from orders
inner join order_stats on orders.id = order_stats.order_id;
