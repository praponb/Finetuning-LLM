SELECT id, amount, status FROM payments WHERE amount >= (SELECT AVG(amount) FROM payments);
