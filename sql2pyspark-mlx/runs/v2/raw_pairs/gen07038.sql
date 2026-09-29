select o.id, o.store_id
from orders o
where o.channel = 'store' and o.customer_id in (select id from customers where age >= 54);
