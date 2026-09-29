SELECT o.id, o.status, o.discount, o.store_id, s.city, s.store_name, s.region FROM orders o JOIN stores s ON s.id = o.store_id ORDER BY o.status DESC
