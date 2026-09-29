SELECT
    CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    product_id,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    COUNT(quantity) AS quantity_count
FROM order_items
WHERE quantity IS NOT NULL
GROUP BY unit_price_bucket, product_id, quantity_bucket
ORDER BY quantity_count;
