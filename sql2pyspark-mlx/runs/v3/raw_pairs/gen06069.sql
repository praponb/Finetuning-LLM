SELECT
    t.id,
    t.txn_time,
    t.merchant,
    a.balance,
    a.opened_date,
    a.status
FROM transactions t
RIGHT OUTER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_type = 'CREDIT' AND a.customer_id IN (20, 24, 27);
