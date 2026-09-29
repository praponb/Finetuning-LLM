WITH src AS (
    SELECT *
    FROM orders
    WHERE channel != 'store'
),
orders_stats AS (
    SELECT channel, ROUND(AVG(amount), 2) AS avg_amount
    FROM src
    GROUP BY channel
)
SELECT *
FROM orders_stats
WHERE avg_amount < 52
ORDER BY avg_amount DESC;
