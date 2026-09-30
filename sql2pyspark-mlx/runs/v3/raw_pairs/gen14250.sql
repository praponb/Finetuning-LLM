SELECT id, amount
FROM transactions
WHERE amount > (SELECT MAX(amount) FROM transactions WHERE account_id IN (23, 30, 39))
ORDER BY id;
