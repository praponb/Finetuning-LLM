SELECT
    t.merchant,
    t.txn_time,
    t.amount,
    a.account_type,
    a.opened_date,
    a.status
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
WHERE a.account_type = 'CREDIT';
