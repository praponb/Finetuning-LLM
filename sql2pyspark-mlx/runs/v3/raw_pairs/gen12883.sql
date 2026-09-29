SELECT i.id, i.quantity, s.sqft, p.product_name, p.price FROM inventory i LEFT JOIN stores s ON i.store_id = s.id INNER JOIN products p ON i.product_id = p.id;
