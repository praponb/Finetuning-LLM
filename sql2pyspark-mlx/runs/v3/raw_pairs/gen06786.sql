SELECT id, amount, order_id, paid_at FROM payments WHERE amount > (SELECT AVG(amount) FROM payments) ORDER BY id;
