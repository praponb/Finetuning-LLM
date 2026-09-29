WITH account_stats AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    WHERE merchant IS NOT NULL
    GROUP BY account_id
)
SELECT
    a.opened_date,
    a.account_type,
    as.stddev_amount,
    as.txn_type_values
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
ORDER BY as.txn_type_values;
