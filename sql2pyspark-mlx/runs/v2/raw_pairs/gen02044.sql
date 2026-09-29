SELECT
    product_id,
    CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    order_id,
    COUNT(DISTINCT quantity) AS unique_orders,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE order_id IN (11, 21)
GROUP BY product_id, CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END, order_id
ORDER BY 6 DESC, product_id DESC
