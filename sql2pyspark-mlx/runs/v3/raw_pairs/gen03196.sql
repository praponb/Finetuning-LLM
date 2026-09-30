SELECT s.id, s.shipped_date, s.carrier, s.status, o.shipping_fee, o.discount FROM shipments s INNER JOIN orders o ON s.order_id = o.id ORDER BY s.shipped_date, id LIMIT 50;
