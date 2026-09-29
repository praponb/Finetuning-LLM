SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(unit_price) AS total_unit_price, COUNT(DISTINCT quantity) AS unique_products
FROM order_items
WHERE product_id = 7 AND unit_price > 110
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
HAVING COUNT(DISTINCT quantity) >= 3
