WITH src AS (
    SELECT *
    FROM transactions
    WHERE account_id IN (1, 14, 21, 22) AND (txn_type NOT IN ('FEE', 'CREDIT') OR amount < 3200)
),
txn_hour_summary AS (
    SELECT HOUR(txn_time) AS txn_hour, COUNT(*) AS total_count
    FROM src
    GROUP BY HOUR(txn_time)
)
SELECT *
FROM txn_hour_summary
WHERE total_count < 4
