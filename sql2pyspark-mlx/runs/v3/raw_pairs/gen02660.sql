SELECT o.discount, s.shipped_date, s.status, s.cost FROM orders o LEFT JOIN shipments s ON s.order_id = o.id ORDER BY shipped_date DESC;
