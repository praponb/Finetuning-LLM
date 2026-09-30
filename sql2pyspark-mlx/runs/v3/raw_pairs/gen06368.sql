SELECT p.id, p.amount FROM payments p WHERE p.order_id NOT IN (SELECT id FROM orders WHERE MONTH(order_date) IN (3, 11, 12))
