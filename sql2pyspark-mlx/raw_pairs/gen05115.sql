WITH account_stats AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) txn_type_values, AVG(amount) avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    a.customer_id,
    as.txn_type_values,
    as.avg_amount
FROM accounts a
JOIN account_stats as ON a.id = as.account_id
ORDER BY as.avg_amount, customer_id DESC, id
LIMIT 10;
