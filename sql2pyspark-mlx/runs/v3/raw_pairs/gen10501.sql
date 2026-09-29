SELECT p.id, p.order_id, p.status FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE discount >= 0.26) ORDER BY 3;
