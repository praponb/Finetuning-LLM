SELECT c.is_active, c.segment, MAX(s.monthly_fee) max_monthly_fee
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active, c.segment;
