SELECT s.id, s.end_date, c.age, c.city FROM subscriptions s JOIN customers c ON s.customer_id = c.id;
