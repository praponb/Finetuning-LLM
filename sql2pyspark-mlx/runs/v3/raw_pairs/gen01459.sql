SELECT unit_price_bucket, unique_orders, cnt
FROM (
    SELECT CASE WHEN unit_price >= 400 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(DISTINCT quantity) AS unique_orders, COUNT(*) AS cnt
    FROM order_items
    WHERE order_id IN (3, 31)
    GROUP BY CASE WHEN unit_price >= 400 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END
) AS s
WHERE unique_orders < 7;
