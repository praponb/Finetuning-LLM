SELECT id, amount, method
FROM payments
WHERE amount = (SELECT MIN(amount) FROM payments)
