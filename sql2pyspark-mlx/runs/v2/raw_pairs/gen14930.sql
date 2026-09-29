select o.id, o.status
from orders o
where o.order_date <= '2024-04-01' and o.customer_id in (select id from customers where city = 'Tokyo')
