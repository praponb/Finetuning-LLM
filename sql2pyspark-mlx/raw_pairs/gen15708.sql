SELECT o.id, o.channel, o.shipping_fee, o.store_id FROM orders o WHERE o.id IN (SELECT order_id FROM payments WHERE amount <= 540)
