SELECT c.is_active, MIN(s.monthly_fee) min_monthly_fee FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id GROUP BY c.is_active
