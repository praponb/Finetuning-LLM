SELECT p.id, p.order_id FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE amount >= 790) ORDER BY id DESC LIMIT 3;
