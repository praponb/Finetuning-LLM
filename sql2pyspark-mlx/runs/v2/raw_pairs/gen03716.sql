SELECT order_id, status, cost
FROM shipments
WHERE (delivered_date >= '2025-03-01' AND shipped_date > '2023-06-01') OR shipped_date BETWEEN '2023-03-03' AND '2024-01-19';
