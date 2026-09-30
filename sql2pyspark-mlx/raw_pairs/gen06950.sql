WITH base AS (
    SELECT *
    FROM transactions
    WHERE merchant = 'Grab'
),
agg AS (
    SELECT txn_type, CASE WHEN amount >= 3600 THEN 'high' WHEN amount >= 600 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(DISTINCT amount) AS unique_merchants
    FROM base
    GROUP BY txn_type, CASE WHEN amount >= 3600 THEN 'high' WHEN amount >= 600 THEN 'mid' ELSE 'low' END
)
SELECT txn_type, amount_bucket
FROM agg
