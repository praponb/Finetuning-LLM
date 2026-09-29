SELECT CASE WHEN unit_price >= 450 THEN 'high' WHEN unit_price >= 90 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MIN(quantity) AS min_quantity
FROM order_items
GROUP BY unit_price_bucket;
