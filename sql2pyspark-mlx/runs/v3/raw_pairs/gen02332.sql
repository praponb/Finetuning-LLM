SELECT
    product_id,
    CASE WHEN units >= 33 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END AS units_bucket,
    AVG(units) AS avg_units,
    COUNT(units) AS units_count
FROM store_sales
WHERE product_id IN (2, 10) AND store_id = 3 AND revenue < 1000
GROUP BY product_id, units_bucket
ORDER BY 4;
