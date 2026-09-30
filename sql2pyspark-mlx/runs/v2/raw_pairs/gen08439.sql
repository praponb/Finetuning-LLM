SELECT i.product_id, s.store_name, s.region, p.category FROM inventory i INNER JOIN stores s ON i.store_id = s.id INNER JOIN products p ON i.product_id = p.id ORDER BY s.region DESC
