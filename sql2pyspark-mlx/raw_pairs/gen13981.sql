SELECT
    CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    product_id,
    CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    MIN(quantity) AS min_quantity
FROM order_items
GROUP BY unit_price_bucket, product_id, quantity_bucket
HAVING MIN(quantity) >= 5.0
ORDER BY min_quantity DESC;
