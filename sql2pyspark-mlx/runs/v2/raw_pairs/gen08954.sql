SELECT unit_price_bucket, avg_unit_price, unique_orders
FROM (
    SELECT CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket, AVG(unit_price) AS avg_unit_price, COUNT(DISTINCT quantity) AS unique_orders
    FROM order_items
    GROUP BY CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END
) AS agg
WHERE unique_orders < 9;
