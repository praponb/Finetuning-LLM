select c.id, c.city from customers c where c.id in (select customer_id from subscriptions)
