select
    o.store_id,
    o.amount,
    o.status,
    oi.order_id,
    oi.quantity
from orders o
left join order_items oi on oi.order_id = o.id
where oi.quantity < 7
order by o.status asc;
