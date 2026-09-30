SELECT method, MAX(paid_at) latest_paid_at, ROUND(AVG(amount), 2) avg_amount
FROM payments
WHERE TO_DATE(paid_at) < '2023-10-15' AND order_id IS NULL
GROUP BY method;
