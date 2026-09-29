SELECT s.id, s.cost, s.order_id, s.shipped_date, o.shipping_fee FROM shipments s INNER JOIN orders o ON s.order_id = o.id ORDER BY s.id DESC;
