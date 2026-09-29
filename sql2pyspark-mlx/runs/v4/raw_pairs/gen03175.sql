SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(*) AS total_count
FROM order_items
WHERE product_id = 6
GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
ORDER BY total_count DESC, quantity_bucket ASC
LIMIT 5;
