SELECT i.product_id, i.store_id, s.sqft, s.store_name, p.stock_qty FROM inventory i LEFT JOIN stores s ON i.store_id = s.id JOIN products p ON i.product_id = p.id
