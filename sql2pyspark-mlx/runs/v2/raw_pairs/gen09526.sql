select o.id, o.customer_id, o.discount, o.status from orders o where o.customer_id not in (select id from customers where credit_limit <= 5400)
