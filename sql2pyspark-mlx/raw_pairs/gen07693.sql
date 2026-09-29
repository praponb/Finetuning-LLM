SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, CASE WHEN unit_price >= 410 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(*) AS num_order_items
FROM order_items
GROUP BY 1, 2
ORDER BY num_order_items DESC;
