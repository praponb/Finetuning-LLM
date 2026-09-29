WITH src AS (
    SELECT *
    FROM transactions
    WHERE account_id = 22
),
summary AS (
    SELECT
        merchant,
        DATE_TRUNC('MONTH', txn_time) AS txn_month,
        COUNT(DISTINCT amount) AS unique_merchants,
        SUM(CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count
    FROM src
    GROUP BY merchant, DATE_TRUNC('MONTH', txn_time)
)
SELECT *
FROM summary
WHERE unique_merchants > 7
ORDER BY unique_merchants DESC, debit_count DESC;
