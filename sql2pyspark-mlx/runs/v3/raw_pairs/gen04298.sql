SELECT method, MIN(paid_at) AS min_paid_at, MAX(amount) - MIN(amount) AS amount_range
FROM payments
WHERE order_id IN (13, 30, 40) AND (amount < 800 OR paid_at < '2023-05-09')
GROUP BY method
HAVING MAX(amount) - MIN(amount) >= 41
