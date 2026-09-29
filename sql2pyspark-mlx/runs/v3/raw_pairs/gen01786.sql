select
    orders.id,
    orders.status,
    orders.discount,
    orders.channel
from orders
where orders.customer_id not in (select id from customers where country in ('DE', 'JP'));
