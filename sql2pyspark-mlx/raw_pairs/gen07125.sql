SELECT a.id, a.opened_date, a.status
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (5, 10, 29, 30))
ORDER BY id DESC
LIMIT 5
