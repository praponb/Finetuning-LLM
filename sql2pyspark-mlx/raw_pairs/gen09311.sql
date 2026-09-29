SELECT id, amount, paid_at FROM payments WHERE amount >= (SELECT MAX(amount) FROM payments)
