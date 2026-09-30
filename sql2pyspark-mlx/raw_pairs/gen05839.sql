WITH base AS (
    SELECT *
    FROM transactions
    WHERE txn_time < '2025-08-01' AND is_flagged
),
transactions_stats AS (
    SELECT merchant, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM base
    GROUP BY merchant
)
SELECT *
FROM transactions_stats
ORDER BY stddev_amount DESC, merchant DESC
