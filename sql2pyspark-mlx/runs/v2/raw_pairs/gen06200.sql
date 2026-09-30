SELECT amount, order_id, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
FROM payments
WHERE method IN ('card', 'cash')
ORDER BY days_since_paid_at DESC
