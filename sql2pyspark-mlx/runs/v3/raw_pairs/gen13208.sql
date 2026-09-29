WITH account_stats AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list, AVG(amount) AS avg_amount
    FROM transactions
    WHERE txn_type NOT IN ('FEE', 'REFUND')
    GROUP BY account_id
)
SELECT
    a.balance,
    a.status,
    as.txn_type_list,
    as.avg_amount
FROM accounts a
LEFT JOIN account_stats as ON a.id = as.account_id
WHERE as.avg_amount >= 3800
