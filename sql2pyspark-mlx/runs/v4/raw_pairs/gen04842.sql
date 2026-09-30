WITH src AS (
    SELECT *
    FROM transactions
    WHERE amount >= 3900
),
agg AS (
    SELECT
        txn_type,
        DATE_TRUNC('MONTH', txn_time) AS txn_month,
        SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount,
        MAX(txn_time) AS latest_txn_time
    FROM src
    GROUP BY txn_type, DATE_TRUNC('MONTH', txn_time)
)
SELECT *
FROM agg
ORDER BY txn_month, latest_txn_time, txn_type
LIMIT 20;
