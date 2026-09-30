SELECT
    p.id,
    p.launch_date,
    t.quantity_count,
    t.stddev_quantity
FROM products p
LEFT JOIN (SELECT product_id, COUNT(quantity) AS quantity_count, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE quantity BETWEEN 134 AND 210
    GROUP BY product_id) t ON p.id = t.product_id
