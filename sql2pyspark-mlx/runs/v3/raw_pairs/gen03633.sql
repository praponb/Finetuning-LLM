SELECT id, amount
FROM payments
WHERE amount <= (SELECT MIN(amount) FROM payments);
