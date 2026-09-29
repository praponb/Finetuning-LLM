SELECT o.order_date, o.store_id, s.store_name, s.city, s.region FROM orders o LEFT JOIN stores s ON o.store_id = s.id;
