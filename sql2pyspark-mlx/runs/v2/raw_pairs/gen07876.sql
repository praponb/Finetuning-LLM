SELECT p.id, p.amount FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE order_date >= '2024-01-21') ORDER BY p.id DESC LIMIT 5;
