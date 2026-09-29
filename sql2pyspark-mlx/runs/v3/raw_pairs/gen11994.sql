SELECT
    CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    SUM(unit_price) AS total_unit_price,
    AVG(quantity) AS avg_quantity,
    MIN(quantity) AS min_quantity
FROM order_items
GROUP BY CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END
ORDER BY unit_price_bucket;
