SELECT p.category, MAX(i.last_restock) AS last_last_restock, SUM(i.quantity) AS total_quantity FROM inventory i INNER JOIN products p ON i.product_id = p.id GROUP BY p.category
