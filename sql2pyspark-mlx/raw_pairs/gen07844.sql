SELECT i.id, i.store_id, s.region, s.city, p.product_name FROM inventory i INNER JOIN stores s ON i.store_id = s.id LEFT JOIN products p ON i.product_id = p.id;
