WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 12 AND 18 AND account_id IN (20, 23, 27, 32)
),
txn_type_summary AS (
    SELECT txn_type, SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END) AS 7_eleven_count
    FROM recent_transactions
    GROUP BY txn_type
)
SELECT *
FROM txn_type_summary
ORDER BY txn_type DESC;
