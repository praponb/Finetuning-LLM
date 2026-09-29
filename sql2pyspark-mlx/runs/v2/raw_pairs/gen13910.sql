SELECT p.id, p.amount FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE shipping_fee BETWEEN 19 AND 29);
