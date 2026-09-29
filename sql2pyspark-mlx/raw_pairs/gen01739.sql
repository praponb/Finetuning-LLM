SELECT CASE WHEN unit_price >= 310 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END AS unit_price_bucket, product_id, COUNT(*) AS row_count
FROM order_items
GROUP BY CASE WHEN unit_price >= 310 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END, product_id;
