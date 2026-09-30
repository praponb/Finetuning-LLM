SELECT p.brand, i.last_restock, i.product_id FROM products p LEFT JOIN inventory i ON i.product_id = p.id ORDER BY p.brand;
