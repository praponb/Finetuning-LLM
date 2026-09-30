SELECT
    t.amount,
    a.balance,
    c.country,
    c.is_active
FROM transactions t
JOIN accounts a ON t.account_id = a.id
JOIN customers c ON a.customer_id = c.id
WHERE t.account_id IN (1, 7, 22, 40) AND t.txn_time >= '2023-12-20'
ORDER BY a.balance DESC
