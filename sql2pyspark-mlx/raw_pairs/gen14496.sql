SELECT CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 90 THEN 'mid' ELSE 'low' END AS unit_price_bucket, product_id, MAX(unit_price) AS max_unit_price
FROM order_items
GROUP BY CASE WHEN unit_price >= 350 THEN 'high' WHEN unit_price >= 90 THEN 'mid' ELSE 'low' END, product_id
HAVING MAX(unit_price) >= 390;
