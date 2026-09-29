SELECT
    a.status,
    a.account_type,
    a.opened_date,
    t.txn_time,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
