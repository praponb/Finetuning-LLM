SELECT CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MIN(unit_price) AS min_unit_price, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE NOT (order_id IN (30, 38) AND product_id = 3)
GROUP BY CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END
ORDER BY unit_price_bucket
