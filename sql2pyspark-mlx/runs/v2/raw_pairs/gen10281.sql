SELECT id, amount, method, status FROM payments WHERE amount > (SELECT MAX(amount) FROM payments);
