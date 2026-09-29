SELECT
    t.id,
    t.txn_type,
    t.txn_time,
    t.is_flagged,
    a.account_type
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.amount BETWEEN 1500 AND 2600;
