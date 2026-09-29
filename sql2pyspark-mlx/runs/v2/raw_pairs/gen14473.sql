SELECT i.store_id, i.product_id, s.sqft, p.brand FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id JOIN products p ON i.product_id = p.id ORDER BY product_id
