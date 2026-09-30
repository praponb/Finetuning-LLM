select c.id, c.city from customers c where c.is_active and exists (select 1 from accounts a where a.customer_id = c.id) order by c.id, city;
