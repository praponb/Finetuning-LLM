SELECT c.is_active, SUM(s.monthly_fee) AS total_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit >= 7500
GROUP BY c.is_active
ORDER BY total_monthly_fee DESC, is_active
LIMIT 1
