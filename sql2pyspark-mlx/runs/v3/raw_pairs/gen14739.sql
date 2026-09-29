select o.status, c.city from orders o inner join customers c on o.customer_id = c.id where c.city in ('Tokyo', 'Chiang Mai', 'Berlin')
