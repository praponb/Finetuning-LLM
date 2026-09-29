SELECT p.category, p.brand, MAX(i.quantity) AS max_quantity FROM inventory i JOIN products p ON i.product_id = p.id GROUP BY p.category, p.brand ORDER BY category ASC
