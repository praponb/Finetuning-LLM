SELECT i.id, i.quantity, s.region, p.product_name, p.supplier_id FROM inventory i LEFT JOIN stores s ON s.id = i.store_id INNER JOIN products p ON i.product_id = p.id;
