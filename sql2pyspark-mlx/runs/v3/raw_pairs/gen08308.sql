SELECT p.brand, MAX(i.quantity) AS max_quantity FROM inventory i JOIN products p ON i.product_id = p.id WHERE i.product_id IN (4, 5, 7, 8) GROUP BY p.brand HAVING MAX(i.quantity) > 100
