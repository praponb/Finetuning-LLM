SELECT COUNT(*) AS total_count, COUNT(DISTINCT amount) AS unique_orders
FROM payments
WHERE (order_id IN (13, 23, 40) AND method NOT IN ('card', 'bank_transfer')) OR order_id IS NOT NULL
ORDER BY 2 DESC
