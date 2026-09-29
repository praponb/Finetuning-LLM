WITH transactions_by_account AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.status, tba.txn_type_list, tba.stddev_amount
FROM accounts a
JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY stddev_amount DESC;
