select c.id, c.is_active, c.segment from customers c where c.age >= 33 and exists (select 1 from accounts a where a.customer_id = c.id);
