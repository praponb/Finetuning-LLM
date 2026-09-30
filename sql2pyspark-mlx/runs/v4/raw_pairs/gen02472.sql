SELECT
    a.opened_date,
    a.status,
    a.customer_id,
    t.txn_time,
    t.account_id,
    t.txn_type
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE a.account_type = 'SAVINGS' AND t.txn_type = 'CREDIT'
ORDER BY customer_id;
