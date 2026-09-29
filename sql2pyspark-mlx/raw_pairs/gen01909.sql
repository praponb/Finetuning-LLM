SELECT a.id, a.status
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (21, 28))
ORDER BY id
