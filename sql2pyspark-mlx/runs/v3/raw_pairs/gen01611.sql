SELECT id, amount
FROM transactions
WHERE amount >= (SELECT MAX(amount) FROM transactions)
ORDER BY id DESC
LIMIT 5
