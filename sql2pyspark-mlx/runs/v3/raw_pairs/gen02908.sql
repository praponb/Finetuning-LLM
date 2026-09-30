SELECT c.is_active, MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range, MIN(s.end_date) AS min_end_date
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active
