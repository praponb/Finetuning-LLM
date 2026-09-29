SELECT product_id, CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MIN(unit_price) AS min_unit_price
FROM order_items
GROUP BY product_id, CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END
ORDER BY unit_price_bucket DESC;
