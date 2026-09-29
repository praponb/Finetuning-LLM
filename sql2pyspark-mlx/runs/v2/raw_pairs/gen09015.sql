SELECT s.id, s.end_date, c.name, c.city, c.email FROM subscriptions s JOIN customers c ON s.customer_id = c.id WHERE c.age <= 67 ORDER BY c.email DESC, s.id
