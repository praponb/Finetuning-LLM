SELECT i.product_id, p.brand FROM inventory i LEFT JOIN products p ON i.product_id = p.id ORDER BY product_id
