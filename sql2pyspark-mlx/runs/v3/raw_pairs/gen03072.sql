SELECT CASE WHEN o.discount >= 0.38 THEN 'high' WHEN o.discount >= 0.16 THEN 'mid' ELSE 'low' END discount_bucket, ROUND(AVG(oi.unit_price), 2) avg_unit_price, ROUND(AVG(oi.quantity), 2) avg_quantity
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY CASE WHEN o.discount >= 0.38 THEN 'high' WHEN o.discount >= 0.16 THEN 'mid' ELSE 'low' END
ORDER BY avg_unit_price DESC;
