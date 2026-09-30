SELECT s.cost, s.order_id, s.status, o.discount, o.shipping_fee, o.store_id FROM shipments s INNER JOIN orders o ON s.order_id = o.id ORDER BY store_id
