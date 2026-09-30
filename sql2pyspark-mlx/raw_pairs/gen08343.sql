SELECT o.shipping_fee, o.discount, s.shipped_date, s.order_id FROM orders o INNER JOIN shipments s ON o.id = s.order_id WHERE s.cost < 81 ORDER BY o.shipping_fee DESC, discount DESC;
