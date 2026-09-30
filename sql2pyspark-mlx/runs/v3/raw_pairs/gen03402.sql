SELECT a.id, a.customer_id
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)
ORDER BY a.customer_id DESC, a.id DESC;
