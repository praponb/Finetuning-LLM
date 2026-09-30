SELECT p.category, COUNT(i.id) AS inventory_count FROM products p LEFT OUTER JOIN inventory i ON i.product_id = p.id WHERE i.product_id IN (5, 6) GROUP BY p.category HAVING COUNT(i.id) >= 4;
