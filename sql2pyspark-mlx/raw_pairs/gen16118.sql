SELECT p.id, COUNT(i.id) AS inventory_count FROM products p LEFT JOIN inventory i ON i.product_id = p.id GROUP BY p.id
