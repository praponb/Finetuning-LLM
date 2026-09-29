WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE channel = 'partner' AND discount < 0.3
),
OrdersStats AS (
    SELECT channel, MAX(order_date) AS latest_order_date
    FROM recent_orders
    GROUP BY channel
)
SELECT channel, latest_order_date
FROM OrdersStats
ORDER BY latest_order_date, channel
LIMIT 10
