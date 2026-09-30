WITH inventory_by_product AS (
    SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.stock_qty,
    p.brand,
    ibp.stddev_quantity,
    ibp.avg_quantity
FROM products p
JOIN inventory_by_product ibp ON p.id = ibp.product_id
ORDER BY p.stock_qty DESC, id
LIMIT 10;
