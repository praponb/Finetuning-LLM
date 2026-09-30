SELECT o.id, o.status, o.channel, o.shipping_fee FROM orders o WHERE o.discount <= 0.16 AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.order_id IN (11, 17, 32, 38));
