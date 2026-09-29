SELECT
    t.id,
    t.is_flagged,
    t.account_id,
    t.txn_time,
    a.balance
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.account_id IN (5, 8, 34);
