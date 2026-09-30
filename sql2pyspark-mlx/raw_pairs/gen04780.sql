SELECT
    CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    SUM(quantity * unit_price) AS revenue,
    AVG(unit_price) AS avg_unit_price,
    MAX(unit_price) AS max_unit_price
FROM order_items
GROUP BY CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END
