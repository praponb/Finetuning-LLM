select c.id, c.signup_date, c.city, c.age from customers c where exists (select 1 from subscriptions s where s.customer_id = c.id and s.customer_id = 36);
