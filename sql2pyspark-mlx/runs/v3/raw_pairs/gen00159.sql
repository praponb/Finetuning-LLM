select o.id, o.discount from orders o where o.customer_id in (select id from customers where segment in ('Corporate', 'Consumer')) order by id limit 10
