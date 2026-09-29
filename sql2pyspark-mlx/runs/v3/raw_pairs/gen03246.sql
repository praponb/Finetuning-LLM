SELECT CASE WHEN o.amount >= 1530 THEN 'high' WHEN o.amount >= 600 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
RIGHT JOIN orders o ON oi.order_id = o.id
WHERE oi.product_id IN (7, 8)
GROUP BY CASE WHEN o.amount >= 1530 THEN 'high' WHEN o.amount >= 600 THEN 'mid' ELSE 'low' END
HAVING avg_unit_price >= 50
ORDER BY avg_unit_price DESC
