WITH src AS (
    SELECT *
    FROM orders
    WHERE customer_id IS NULL
),
summary AS (
    SELECT channel, MIN(order_date) AS min_order_date
    FROM src
    GROUP BY channel
)
SELECT *
FROM summary
ORDER BY channel, min_order_date
LIMIT 5
