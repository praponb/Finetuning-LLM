SELECT i.id, i.last_restock, p.brand, s.region, s.store_name FROM inventory i INNER JOIN products p ON i.product_id = p.id LEFT OUTER JOIN stores s ON i.store_id = s.id ORDER BY last_restock, brand;
