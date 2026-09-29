SELECT method, MIN(amount) min_amount
FROM payments
WHERE order_id IS NOT NULL AND order_id IS NULL
GROUP BY method;
