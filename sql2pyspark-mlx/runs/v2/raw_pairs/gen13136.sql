SELECT a.id, a.status, a.balance
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions)
ORDER BY 3 DESC
