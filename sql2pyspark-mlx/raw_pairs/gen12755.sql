WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE store_id IS NOT NULL AND store_id IN (11, 34)
),
orders_stats AS (
    SELECT status, ROUND(AVG(amount), 2) AS avg_amount
    FROM recent_orders
    GROUP BY status
)
SELECT status, avg_amount
FROM orders_stats
ORDER BY 1;
