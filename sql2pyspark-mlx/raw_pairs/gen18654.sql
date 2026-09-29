WITH account_summary AS (
    SELECT account_id, MIN(txn_time) first_txn_time, COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) refund_count
    FROM transactions
    GROUP BY account_id
)
SELECT accounts.account_type, account_summary.first_txn_time, account_summary.refund_count
FROM accounts
JOIN account_summary ON accounts.id = account_summary.account_id
WHERE account_summary.refund_count > 3;
