SELECT
    product_id,
    CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END unit_price_bucket,
    ROUND(AVG(quantity), 2) avg_quantity,
    MIN(quantity) min_quantity,
    ROUND(STDDEV(unit_price), 2) stddev_unit_price
FROM order_items
WHERE unit_price BETWEEN 390 AND 400
GROUP BY product_id, unit_price_bucket
ORDER BY 4 DESC
