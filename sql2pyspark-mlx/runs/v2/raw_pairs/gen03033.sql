SELECT p.id, p.method, p.order_id FROM payments p WHERE TO_DATE(p.paid_at) < '2024-11-14' AND p.order_id IN (SELECT id FROM orders WHERE shipping_fee >= 34) ORDER BY order_id DESC;
