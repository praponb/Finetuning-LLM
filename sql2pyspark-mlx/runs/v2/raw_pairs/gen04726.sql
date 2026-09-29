WITH src AS (
    SELECT *
    FROM transactions
    WHERE amount BETWEEN 3000 AND 3300 AND amount < 3000
),
agg AS (
    SELECT txn_type, ROUND((100 * SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fee, MAX(amount) AS max_amount
    FROM src
    GROUP BY txn_type
)
SELECT *
FROM agg
WHERE max_amount > 2900
ORDER BY pct_fee, txn_type
LIMIT 20;
