select c.id, c.age, c.is_active, c.name from customers c where c.country = 'DE' and exists (select 1 from orders o where o.customer_id = c.id)
