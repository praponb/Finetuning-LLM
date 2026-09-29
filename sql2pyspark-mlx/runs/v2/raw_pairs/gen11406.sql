SELECT
    p.id,
    p.cost,
    p.brand,
    t.stddev_quantity,
    t.quantity_count
FROM products p
INNER JOIN (SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, COUNT(quantity) AS quantity_count
    FROM inventory
    WHERE store_id IN (3, 4, 6)
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.quantity_count > 6
ORDER BY p.cost DESC
