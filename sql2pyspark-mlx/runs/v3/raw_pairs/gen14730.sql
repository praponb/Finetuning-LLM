SELECT oi.id, oi.order_id, o.status, o.store_id, s.region FROM order_items oi LEFT OUTER JOIN orders o ON o.id = oi.order_id INNER JOIN stores s ON o.store_id = s.id;
