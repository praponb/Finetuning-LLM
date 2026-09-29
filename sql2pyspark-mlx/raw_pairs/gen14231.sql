SELECT
    CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 190 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    COUNT(unit_price) AS unit_price_count,
    AVG(quantity) AS avg_quantity,
    MIN(unit_price) AS min_unit_price
FROM order_items
GROUP BY 1
ORDER BY unit_price_bucket
LIMIT 50;
