SELECT p.category, COUNT(i.id) AS inventory_count, MAX(i.quantity) AS max_quantity FROM products p LEFT OUTER JOIN inventory i ON i.product_id = p.id GROUP BY p.category;
