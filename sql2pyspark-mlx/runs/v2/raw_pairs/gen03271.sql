SELECT s.id, s.status, s.carrier, s.order_id, o.shipping_fee, o.store_id, o.amount FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE o.channel IN ('mobile', 'partner', 'web');
