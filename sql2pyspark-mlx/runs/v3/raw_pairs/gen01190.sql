WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE customer_id IS NULL AND status = 'PENDING'
),
summary AS (
    SELECT store_id, COUNT(discount) AS discount_count
    FROM recent_orders
    GROUP BY store_id
)
SELECT store_id, discount_count
FROM summary
ORDER BY discount_count DESC;
