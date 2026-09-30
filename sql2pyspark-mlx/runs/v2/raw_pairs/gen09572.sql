SELECT c.is_active, MIN(s.monthly_fee) AS min_monthly_fee FROM subscriptions s INNER JOIN customers c ON c.id = s.customer_id GROUP BY c.is_active ORDER BY min_monthly_fee DESC
