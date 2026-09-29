SELECT CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(*) AS cnt, MAX(unit_price) AS max_unit_price
FROM order_items
WHERE unit_price >= 350 AND product_id = 14
GROUP BY quantity_bucket
HAVING AVG(unit_price) >= 350
