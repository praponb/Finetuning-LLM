SELECT p.id, p.product_name, COUNT(i.id) inventory_count FROM products p LEFT JOIN inventory i ON i.product_id = p.id GROUP BY p.id, p.product_name;
