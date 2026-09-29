with order_totals as (
    select order_id, max(unit_price) max_unit_price
    from order_items
    where unit_price between 280 and 320
    group by order_id
)
select orders.id, orders.channel, order_totals.max_unit_price
from orders
left join order_totals on orders.id = order_totals.order_id
where order_totals.max_unit_price > 180;
