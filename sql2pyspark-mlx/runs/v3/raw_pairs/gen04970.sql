SELECT product_id, CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
GROUP BY product_id, CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END
ORDER BY product_id DESC
