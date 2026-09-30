SELECT c.is_active, c.city, MIN(s.start_date) first_start_date FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id GROUP BY c.is_active, c.city
