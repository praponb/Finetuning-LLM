SELECT
    CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    MAX(quantity) AS max_quantity,
    COUNT(DISTINCT quantity) AS unique_products,
    SUM(quantity) AS total_quantity
FROM order_items
WHERE order_id IN (18, 35)
GROUP BY unit_price_bucket
