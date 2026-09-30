SELECT a.id, a.status
FROM accounts a
WHERE a.customer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (13, 16, 17, 32));
