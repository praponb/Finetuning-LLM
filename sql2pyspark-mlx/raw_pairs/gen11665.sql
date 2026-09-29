select o.id, o.store_id, o.status from orders o where o.customer_id in (select id from customers where year(signup_date) = 2025)
