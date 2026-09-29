SELECT a.id, a.balance
FROM accounts a
WHERE a.opened_date < '2025-02-01' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)
ORDER BY balance
