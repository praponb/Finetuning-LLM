SELECT *
FROM (
    SELECT CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 80 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(DISTINCT unit_price) AS unique_products
    FROM order_items
    GROUP BY CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 80 THEN 'mid' ELSE 'low' END
) AS agg
WHERE unique_products < 6
ORDER BY unit_price_bucket
