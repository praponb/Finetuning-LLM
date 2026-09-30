SELECT
    a.id,
    a.status,
    a.customer_id,
    a.balance
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount BETWEEN 2000 AND 3000)
