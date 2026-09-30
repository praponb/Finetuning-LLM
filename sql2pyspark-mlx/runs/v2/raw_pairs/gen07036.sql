select c.id, c.email, c.segment from customers c where c.id in (select customer_id from accounts) order by 2, id limit 5
