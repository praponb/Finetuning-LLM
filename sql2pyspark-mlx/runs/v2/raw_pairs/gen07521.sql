SELECT
    p.id,
    p.launch_date,
    t.total_quantity,
    t.avg_quantity
FROM products p
JOIN (SELECT product_id, SUM(quantity) total_quantity, AVG(quantity) avg_quantity
    FROM inventory
    WHERE YEAR(last_restock) = 2024
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.total_quantity > 540
ORDER BY id DESC;
