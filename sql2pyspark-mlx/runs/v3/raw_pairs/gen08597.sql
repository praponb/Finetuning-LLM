SELECT i.product_id, i.store_id, p.cost, s.city, s.region FROM inventory i LEFT JOIN products p ON i.product_id = p.id LEFT JOIN stores s ON i.store_id = s.id ORDER BY s.city DESC;
