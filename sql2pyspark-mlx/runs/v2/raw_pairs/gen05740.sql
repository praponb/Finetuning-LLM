SELECT p.id, p.category, COUNT(i.id) AS inventory_count FROM products p LEFT JOIN inventory i ON i.product_id = p.id GROUP BY p.id, p.category ORDER BY category, inventory_count DESC, id LIMIT 50;
