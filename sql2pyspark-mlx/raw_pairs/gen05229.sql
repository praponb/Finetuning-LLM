SELECT product_id, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE unit_price >= 120
GROUP BY ROLLUP(product_id, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END)
ORDER BY quantity_bucket
