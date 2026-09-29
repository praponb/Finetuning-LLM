SELECT
    a.id,
    a.status,
    a.balance,
    a.customer_id
FROM accounts a
WHERE a.account_type = 'LOAN' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (20, 40))
ORDER BY a.id DESC;
