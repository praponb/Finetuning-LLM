SELECT id, amount, paid_at, method FROM payments WHERE amount > (SELECT MAX(amount) FROM payments);
