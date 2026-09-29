SELECT
    CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 150 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    SUM(quantity) AS total_quantity,
    AVG(quantity) AS avg_quantity,
    AVG(unit_price) AS avg_unit_price
FROM order_items
WHERE order_id IN (7, 18, 39) AND quantity <> 8
GROUP BY CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 150 THEN 'mid' ELSE 'low' END;
