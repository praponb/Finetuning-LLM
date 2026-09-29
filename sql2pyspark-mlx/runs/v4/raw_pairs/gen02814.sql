SELECT
    product_id,
    CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    SUM(quantity) AS total_quantity
FROM order_items
WHERE product_id = 38 AND order_id IN (7, 17, 22, 23)
GROUP BY product_id, CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
ORDER BY quantity_bucket DESC;
