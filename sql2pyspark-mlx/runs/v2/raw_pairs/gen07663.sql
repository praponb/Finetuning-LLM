SELECT s.city, AVG(i.quantity) AS avg_quantity FROM inventory i LEFT JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON s.id = p.supplier_id GROUP BY s.city ORDER BY avg_quantity DESC;
