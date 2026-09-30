select oi.id, oi.quantity
from order_items oi
where oi.order_id in (8, 11, 21) and oi.order_id in (select id from orders where year(order_date) = 2023);
