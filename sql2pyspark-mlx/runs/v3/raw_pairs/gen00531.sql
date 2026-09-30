SELECT CASE WHEN o.discount >= 0.38 THEN 'high' WHEN o.discount >= 0.13 THEN 'mid' ELSE 'low' END AS discount_bucket, SUM(oi.unit_price * oi.quantity) AS revenue, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE o.store_id = 36
GROUP BY CASE WHEN o.discount >= 0.38 THEN 'high' WHEN o.discount >= 0.13 THEN 'mid' ELSE 'low' END
HAVING ROUND(STDDEV(oi.quantity), 2) >= 44
ORDER BY revenue DESC;
