SELECT id, amount, method, order_id FROM payments WHERE amount >= (SELECT AVG(amount) FROM payments);
