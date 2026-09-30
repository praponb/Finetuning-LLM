WITH base AS (
    SELECT *
    FROM transactions
    WHERE txn_time < '2025-03-01' AND merchant IN ('Amazon', 'Grab', 'Lazada')
),
agg AS (
    SELECT TO_DATE(txn_time) AS txn_day, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM base
    GROUP BY TO_DATE(txn_time)
)
SELECT txn_day, merchant_list
FROM agg
ORDER BY txn_day
LIMIT 20
