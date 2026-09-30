WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE (txn_type <> 'DEBIT' AND txn_time < '2025-10-06') OR account_id IN (9, 23, 24, 29)
),
agg AS (
    SELECT DATE_TRUNC('MONTH', txn_time) AS txn_month, COUNT(DISTINCT amount) AS unique_accounts, SUM(CASE WHEN merchant = 'Grab' THEN amount ELSE 0 END) AS grab_amount
    FROM valid_transactions
    GROUP BY DATE_TRUNC('MONTH', txn_time)
)
SELECT txn_month, grab_amount
FROM agg
ORDER BY txn_month, grab_amount DESC
