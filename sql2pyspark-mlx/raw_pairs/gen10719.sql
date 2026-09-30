SELECT
    a.account_type,
    a.status,
    a.opened_date,
    t.merchant,
    t.is_flagged,
    t.txn_time
FROM accounts a
JOIN transactions t ON t.account_id = a.id
WHERE a.balance BETWEEN 38000 AND 46000
ORDER BY t.txn_time DESC, status DESC;
