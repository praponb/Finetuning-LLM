WITH base AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (10, 27)
),
transactions_stats AS (
    SELECT merchant, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM base
    GROUP BY merchant
)
SELECT *
FROM transactions_stats
ORDER BY merchant, stddev_amount DESC
LIMIT 50
