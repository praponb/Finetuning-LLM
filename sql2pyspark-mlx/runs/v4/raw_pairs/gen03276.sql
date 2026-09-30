SELECT s.id, s.start_date, s.monthly_fee, c.email FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE YEAR(s.end_date) >= 2023
