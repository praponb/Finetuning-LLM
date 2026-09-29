SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, order_id, ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
WHERE quantity >= 8
GROUP BY 1, 2
ORDER BY order_id DESC, quantity_bucket
LIMIT 5
