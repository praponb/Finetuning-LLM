SELECT
    t.id,
    t.amount,
    a.account_type,
    a.balance,
    c.city,
    c.is_active
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE t.account_id IN (10, 27) OR t.txn_time < '2024-07-25'
ORDER BY t.id DESC
LIMIT 10;
