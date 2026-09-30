SELECT
    t.id,
    t.account_id,
    t.amount,
    a.balance
FROM transactions t
JOIN accounts a ON a.id = t.account_id
ORDER BY balance DESC
