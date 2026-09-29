SELECT p.category, MIN(i.quantity) AS min_quantity FROM inventory i LEFT JOIN products p ON i.product_id = p.id GROUP BY p.category ORDER BY p.category
