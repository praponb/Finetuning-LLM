SELECT p.launch_date, t.min_quantity, t.avg_quantity
FROM products p
LEFT JOIN (SELECT product_id, MIN(quantity) min_quantity, AVG(quantity) avg_quantity
    FROM inventory
    WHERE quantity BETWEEN 100 AND 202
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY t.avg_quantity DESC
