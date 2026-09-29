WITH account_totals AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM transactions
    GROUP BY account_id
)
SELECT a.account_type, a.customer_id, at.txn_type_list
FROM accounts a
LEFT OUTER JOIN account_totals at ON a.id = at.account_id
ORDER BY a.customer_id ASC;
