SELECT
    CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    order_id,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END, CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END, order_id;
