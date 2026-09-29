SELECT a.id, a.balance, a.status
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.is_flagged)
ORDER BY status DESC, id;
