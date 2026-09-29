SELECT status, order_id
FROM payments
WHERE paid_at < '2023-03-01'
UNION ALL
SELECT status, order_id
FROM payments
WHERE TO_DATE(paid_at) >= '2024-09-09';
