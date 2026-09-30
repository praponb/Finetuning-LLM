select o.id, o.discount, o.amount, o.status from orders o where o.customer_id in (select id from customers where age <> 61);
