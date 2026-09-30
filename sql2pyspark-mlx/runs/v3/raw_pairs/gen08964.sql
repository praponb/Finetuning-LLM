SELECT order_id, MIN(shipped_date) AS first_shipped_date
FROM shipments
WHERE YEAR(delivered_date) = 2024 AND order_id = 5 AND delivered_date > '2025-03-01'
GROUP BY order_id
ORDER BY order_id DESC;
