SELECT id, amount, method FROM payments WHERE amount >= (SELECT AVG(amount) FROM payments WHERE order_id = 28) ORDER BY method
