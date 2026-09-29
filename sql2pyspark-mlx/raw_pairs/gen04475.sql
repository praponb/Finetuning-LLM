SELECT s.id, s.monthly_fee, s.status, s.start_date, c.is_active FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE s.end_date <= '2023-08-01'
