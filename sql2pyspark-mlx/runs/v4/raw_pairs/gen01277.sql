select o.status, o.amount, oi.order_id
from orders o
left join order_items oi on oi.order_id = o.id
order by order_id desc;
