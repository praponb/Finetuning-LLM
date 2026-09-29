SELECT HOUR(paid_at) AS paid_hour, COUNT(DISTINCT amount) AS unique_orders
FROM payments
WHERE amount >= 480 AND order_id = 3 AND order_id IS NOT NULL
GROUP BY 1
HAVING unique_orders > 3;
