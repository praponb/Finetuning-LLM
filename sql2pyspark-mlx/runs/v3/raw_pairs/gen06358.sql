select oi.id, oi.quantity, oi.unit_price
from order_items oi
where oi.order_id in (select id from orders where customer_id in (5, 9, 28, 33))
order by oi.unit_price desc;
