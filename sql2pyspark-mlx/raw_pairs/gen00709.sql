SELECT method, paid_at, order_id
FROM payments
WHERE paid_at >= '2024-01-01' AND (amount <= 850 OR amount < 850)
ORDER BY order_id ASC;
