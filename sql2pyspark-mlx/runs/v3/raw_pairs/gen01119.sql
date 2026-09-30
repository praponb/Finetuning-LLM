select customers.id, customers.signup_date, customers.country from customers where customers.age != 50 and customers.id in (select customer_id from subscriptions)
