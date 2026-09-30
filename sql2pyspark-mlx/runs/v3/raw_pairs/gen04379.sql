SELECT s.order_id, o.channel FROM shipments s LEFT OUTER JOIN orders o ON o.id = s.order_id WHERE o.shipping_fee <= 11 AND s.order_id IN (11, 36, 37)
