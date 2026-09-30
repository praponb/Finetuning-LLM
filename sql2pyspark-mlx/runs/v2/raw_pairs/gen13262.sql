SELECT a.id, a.customer_id
FROM accounts a
WHERE a.balance > 26000 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
