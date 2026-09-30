SELECT
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    product_id,
    MAX(unit_price) AS max_unit_price,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
GROUP BY ROLLUP(CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END, product_id)
