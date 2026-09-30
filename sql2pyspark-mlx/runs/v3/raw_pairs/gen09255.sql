SELECT
    product_id,
    MIN(quantity) AS min_quantity,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY product_id
ORDER BY product_id DESC
