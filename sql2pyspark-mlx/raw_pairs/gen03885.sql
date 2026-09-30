SELECT p.order_id, o.discount, o.shipping_fee, o.customer_id FROM payments p JOIN orders o ON p.order_id = o.id WHERE p.order_id IN (14, 18, 29) OR p.order_id IS NOT NULL ORDER BY discount DESC
