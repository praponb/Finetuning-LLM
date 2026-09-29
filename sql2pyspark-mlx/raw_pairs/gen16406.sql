SELECT
    order_id,
    CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    product_id,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
GROUP BY order_id, CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END, product_id
ORDER BY order_id DESC;
