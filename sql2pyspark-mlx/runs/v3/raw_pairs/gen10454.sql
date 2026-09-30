SELECT p.id, p.status, p.amount, p.order_id FROM payments p WHERE p.method != 'cash' AND p.order_id NOT IN (SELECT id FROM orders WHERE order_date <= '2024-12-01') ORDER BY 4 ASC
