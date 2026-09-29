WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE customer_id IS NULL AND status = 'PENDING'
),
summary AS (
    SELECT channel, COUNT(discount) AS discount_count
    FROM recent_orders
    GROUP BY channel
)
SELECT channel, discount_count
FROM summary
ORDER BY discount_count DESC;
