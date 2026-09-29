SELECT i.id, i.store_id, p.product_name, p.brand, s.city FROM inventory i LEFT JOIN products p ON i.product_id = p.id FULL OUTER JOIN stores s ON i.store_id = s.id;
