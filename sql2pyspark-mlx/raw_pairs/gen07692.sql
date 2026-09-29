SELECT status
FROM payments
WHERE paid_at < '2024-02-27'
UNION
SELECT status
FROM shipments
WHERE order_id IN (14, 18, 24, 40);
