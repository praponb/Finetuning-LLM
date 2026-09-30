SELECT id, is_flagged
FROM transactions
WHERE amount <= 3000
ORDER BY id
LIMIT 10;
