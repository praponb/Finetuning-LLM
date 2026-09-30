WITH src AS (
    SELECT *
    FROM orders
    WHERE discount < 0.26
),
orders_stats AS (
    SELECT status, ROUND(AVG(amount), 2) AS avg_amount, MAX(amount) AS max_amount
    FROM src
    GROUP BY status
)
SELECT status, avg_amount, max_amount
FROM orders_stats
WHERE avg_amount < 29
ORDER BY status ASC, max_amount DESC
