SELECT
    order_id,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(*) AS total_count,
    MIN(unit_price) AS min_unit_price
FROM order_items
WHERE product_id IS NOT NULL
GROUP BY order_id, quantity_bucket;
