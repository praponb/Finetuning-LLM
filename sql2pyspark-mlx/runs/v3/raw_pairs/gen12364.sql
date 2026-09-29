SELECT CASE WHEN o.amount >= 1590 THEN 'high' WHEN o.amount >= 440 THEN 'mid' ELSE 'low' END amount_bucket, ROUND(STDDEV(oi.quantity), 2) stddev_quantity, ROUND(AVG(oi.quantity), 2) avg_quantity
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
FULL OUTER JOIN products p ON oi.product_id = p.id
WHERE oi.order_id IN (8, 18, 26, 27)
GROUP BY CASE WHEN o.amount >= 1590 THEN 'high' WHEN o.amount >= 440 THEN 'mid' ELSE 'low' END
HAVING ROUND(AVG(oi.quantity), 2) >= 28;
