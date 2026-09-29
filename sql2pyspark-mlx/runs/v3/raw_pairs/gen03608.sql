SELECT
    ss.store_id,
    CASE WHEN ss.units >= 44 THEN 'high' WHEN ss.units >= 20 THEN 'mid' ELSE 'low' END AS units_bucket,
    AVG(i.quantity) AS avg_quantity,
    COUNT(i.quantity) AS quantity_count
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units = 18
GROUP BY ss.store_id, CASE WHEN ss.units >= 44 THEN 'high' WHEN ss.units >= 20 THEN 'mid' ELSE 'low' END;
