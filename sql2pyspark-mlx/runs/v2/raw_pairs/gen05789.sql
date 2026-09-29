select orders.id, orders.discount, orders.shipping_fee
from orders
where orders.customer_id in (select id from customers where age = 38);
