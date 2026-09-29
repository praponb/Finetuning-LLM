SELECT id, merchant, amount
FROM transactions
WHERE account_id IN (20, 32) OR amount <= 3300;
