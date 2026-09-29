WITH account_totals AS (
    SELECT account_id, AVG(amount) AS avg_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    at.avg_amount,
    at.txn_type_values
FROM accounts a
JOIN account_totals at ON a.id = at.account_id
WHERE at.avg_amount > 3600
ORDER BY a.id;
