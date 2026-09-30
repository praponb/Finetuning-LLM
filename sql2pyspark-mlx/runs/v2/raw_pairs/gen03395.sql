SELECT id, amount, paid_at FROM payments WHERE amount > (SELECT AVG(amount) FROM payments)
