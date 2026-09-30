SELECT
    t.id,
    t.merchant,
    a.opened_date,
    a.balance
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE a.balance <= 43000 AND a.balance >= 42000;
