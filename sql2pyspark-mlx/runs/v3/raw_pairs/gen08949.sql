SELECT
    t.txn_time,
    t.txn_type,
    t.is_flagged,
    a.status
FROM transactions t
JOIN accounts a ON t.account_id = a.id
ORDER BY t.txn_type DESC, status;
