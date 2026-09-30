SELECT s.id, s.start_date, s.end_date, c.city, c.age, c.segment FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id ORDER BY c.city DESC, c.age, id LIMIT 20
