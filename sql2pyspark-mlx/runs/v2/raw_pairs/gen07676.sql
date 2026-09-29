select o.amount, o.customer_id, oi.order_id
from orders o
left join order_items oi on o.id = oi.order_id
