SELECT s.end_date, s.status, s.plan, c.name, c.age, c.city FROM subscriptions s JOIN customers c ON s.customer_id = c.id ORDER BY status DESC;
