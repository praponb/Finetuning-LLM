SELECT
    CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 60 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    AVG(quantity) AS avg_quantity,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM order_items
GROUP BY CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 60 THEN 'mid' ELSE 'low' END
