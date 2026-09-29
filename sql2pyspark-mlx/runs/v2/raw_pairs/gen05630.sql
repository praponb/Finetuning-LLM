SELECT
    t.txn_time,
    t.merchant,
    t.txn_type,
    a.opened_date,
    a.customer_id,
    a.status
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
WHERE t.account_id IN (1, 15);
