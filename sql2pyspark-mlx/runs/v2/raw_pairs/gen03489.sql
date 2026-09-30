SELECT id, amount, order_id, method FROM payments WHERE amount >= (SELECT MAX(amount) FROM payments WHERE order_id IN (4, 11, 12, 37));
