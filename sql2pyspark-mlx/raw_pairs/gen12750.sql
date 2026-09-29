SELECT
    a.status,
    a.account_type,
    SORT_ARRAY(COLLECT_SET(t.txn_type)) txn_type_list,
    SUM(t.amount) total_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.status <> 'CLOSED'
GROUP BY a.status, a.account_type
