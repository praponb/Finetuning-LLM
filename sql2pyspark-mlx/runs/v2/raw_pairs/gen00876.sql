SELECT id, amount, order_id FROM payments WHERE amount = (SELECT MIN(amount) FROM payments)
