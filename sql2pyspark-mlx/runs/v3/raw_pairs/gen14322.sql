SELECT MIN(amount) min_amount
FROM payments
WHERE paid_at < '2025-05-01' AND HOUR(paid_at) BETWEEN 10 AND 13 AND order_id = 12;
