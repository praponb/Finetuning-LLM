SELECT unit_price_bucket, unit_price_range
FROM (
    SELECT CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 90 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    WHERE order_id IN (2, 20, 21)
    GROUP BY CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 90 THEN 'mid' ELSE 'low' END
) AS s
WHERE unit_price_range > 49
ORDER BY unit_price_range;
