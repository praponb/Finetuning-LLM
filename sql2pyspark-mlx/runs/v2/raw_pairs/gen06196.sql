SELECT
    a.id,
    a.account_type,
    a.balance,
    t.stddev_amount,
    t.txn_type_list
FROM accounts a
INNER JOIN (SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY balance DESC, id
LIMIT 5
