SELECT
    CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    order_id,
    COUNT(DISTINCT quantity) AS unique_products,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
GROUP BY 1, 2
HAVING MAX(quantity) - MIN(quantity) > 64
ORDER BY unit_price_bucket, order_id
LIMIT 1
