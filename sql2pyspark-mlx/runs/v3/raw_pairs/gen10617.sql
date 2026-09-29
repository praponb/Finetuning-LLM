WITH filtered_orders AS (
    SELECT *, shipping_fee + amount AS amount_with_shipping
    FROM orders
    WHERE customer_id IS NOT NULL OR store_id IS NULL
),
summary AS (
    SELECT channel, status, COUNT(discount) AS discount_count
    FROM filtered_orders
    GROUP BY channel, status
)
SELECT *
FROM summary
WHERE discount_count < 9
ORDER BY status, channel
LIMIT 5
