SELECT s.id, s.status, s.order_id, o.channel, o.store_id, o.shipping_fee, COALESCE(s.cost, 0) AS cost_filled FROM shipments s LEFT JOIN orders o ON o.id = s.order_id WHERE o.shipping_fee <= 17;
