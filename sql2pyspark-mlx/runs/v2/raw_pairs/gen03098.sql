SELECT id, amount FROM payments WHERE amount > (SELECT MAX(amount) FROM payments WHERE method IS NOT NULL) ORDER BY amount DESC
