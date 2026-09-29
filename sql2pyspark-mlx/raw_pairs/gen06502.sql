SELECT id, amount, order_id FROM payments WHERE amount > (SELECT AVG(amount) FROM payments WHERE amount >= 1000)
