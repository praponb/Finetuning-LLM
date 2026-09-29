SELECT
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    AVG(unit_price) AS avg_unit_price,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    MIN(quantity) AS min_quantity
FROM order_items
WHERE product_id = 32 AND (quantity BETWEEN 4 AND 6 OR unit_price <= 170)
GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
HAVING COUNT(quantity) > 1;
