SELECT p.id, p.order_id FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE order_date >= '2023-03-01');
