SELECT status, order_count, unique_stores
FROM (
    SELECT status, COUNT(*) AS order_count, COUNT(DISTINCT discount) AS unique_stores
    FROM orders
    GROUP BY status
) AS sub
WHERE unique_stores > 9
ORDER BY order_count, status
LIMIT 5
