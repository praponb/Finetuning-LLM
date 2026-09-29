select o.id, o.amount, c.country, c.is_active, c.credit_limit from orders o inner join customers c on c.id = o.customer_id where o.store_id in (37, 38, 40) order by o.amount desc, id limit 20;
