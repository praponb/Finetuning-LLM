SELECT o.id, o.shipping_fee, o.order_date
FROM orders o
WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.amount <= 1190);
