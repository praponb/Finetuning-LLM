select o.id, o.channel, c.country, c.age, c.name from orders o inner join customers c on o.customer_id = c.id where not (c.age <= 42 and o.order_date < '2025-04-01')
