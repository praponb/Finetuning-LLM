WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE channel = 'web'
),
orders_stats AS (
    SELECT channel, status, COUNT(DISTINCT shipping_fee) AS unique_channels
    FROM recent_orders
    GROUP BY channel, status
)
SELECT channel, status, unique_channels
FROM orders_stats
WHERE unique_channels > 8;
