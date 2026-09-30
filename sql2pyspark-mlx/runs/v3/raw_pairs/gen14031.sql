SELECT p.id, p.status, p.amount, p.method FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.14 AND 0.19) ORDER BY method ASC
