select c.id, c.age, c.email from customers c where c.id in (select customer_id from accounts where month(opened_date) in (3, 6, 8))
