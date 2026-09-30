WITH FilteredTransactions AS (
    SELECT *, UPPER(txn_type) AS txn_type_upper
    FROM transactions
    WHERE txn_time < '2024-01-05' AND account_id IN (21, 36)
),
agg AS (
    SELECT txn_type, COUNT(*) AS num_transactions
    FROM FilteredTransactions
    GROUP BY txn_type
)
SELECT *
FROM agg
WHERE num_transactions > 7
ORDER BY txn_type DESC;
