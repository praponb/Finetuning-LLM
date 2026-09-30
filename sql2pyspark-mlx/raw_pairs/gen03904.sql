WITH src AS (
    SELECT *
    FROM orders
    WHERE status = 'COMPLETED'
),
summary AS (
    SELECT channel, COUNT(amount) AS amount_count
    FROM src
    GROUP BY channel
)
SELECT channel, amount_count
FROM summary
ORDER BY amount_count DESC;
