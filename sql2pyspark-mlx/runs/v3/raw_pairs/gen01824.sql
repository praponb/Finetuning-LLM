SELECT a.id, a.account_type
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions WHERE amount >= 3100)
ORDER BY account_type, id
LIMIT 10;
