SELECT id, amount
FROM payments
WHERE amount >= (SELECT AVG(amount) FROM payments WHERE method IS NOT NULL);
