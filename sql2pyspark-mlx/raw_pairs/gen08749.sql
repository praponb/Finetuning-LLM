SELECT p.id, p.paid_at, p.amount FROM payments p WHERE p.amount < 790 AND p.order_id NOT IN (SELECT id FROM orders WHERE discount <= 0.39);
