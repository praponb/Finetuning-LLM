SELECT id, amount, order_id, method FROM payments WHERE amount < (SELECT MIN(amount) FROM payments)
