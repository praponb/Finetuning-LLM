SELECT
    order_id,
    product_id,
    CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    SUM(quantity * unit_price) AS revenue
FROM order_items
GROUP BY order_id, product_id, CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END;
