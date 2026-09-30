SELECT id, amount, method
FROM payments
WHERE amount > (SELECT AVG(amount) FROM payments)
