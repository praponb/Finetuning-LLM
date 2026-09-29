SELECT
    a.id,
    a.customer_id,
    a.status,
    a.account_type
FROM accounts a
WHERE a.status = 'OPEN' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
