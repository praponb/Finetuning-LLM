WITH account_stats AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) txn_type_list, AVG(amount) avg_amount
    FROM transactions
    WHERE amount BETWEEN 3500 AND 4000
    GROUP BY account_id
)
SELECT a.opened_date, as.txn_type_list, as.avg_amount
FROM accounts a
LEFT OUTER JOIN account_stats as ON a.id = as.account_id
WHERE as.avg_amount > 3500
ORDER BY as.avg_amount
