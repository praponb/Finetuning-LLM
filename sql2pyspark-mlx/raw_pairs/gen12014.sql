SELECT
    CASE WHEN discount >= 0.34 THEN 'high' WHEN discount >= 0.14 THEN 'mid' ELSE 'low' END AS discount_bucket,
    status,
    COUNT(*) AS num_orders,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count
FROM orders
WHERE order_date > '2025-04-01' AND amount > 1430
GROUP BY CASE WHEN discount >= 0.34 THEN 'high' WHEN discount >= 0.14 THEN 'mid' ELSE 'low' END, status
HAVING SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) >= 72
