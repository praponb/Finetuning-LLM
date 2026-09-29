SELECT
    a.status,
    t.txn_time,
    t.amount,
    t.is_flagged,
    CONCAT(a.status, ' ', a.account_type) AS status_account_type
FROM accounts a
LEFT OUTER JOIN transactions t ON t.account_id = a.id
WHERE t.txn_type = 'CREDIT'
ORDER BY status_account_type
