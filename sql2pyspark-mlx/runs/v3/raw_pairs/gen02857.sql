SELECT CASE WHEN o.discount >= 0.44 THEN 'high' WHEN o.discount >= 0.12 THEN 'mid' ELSE 'low' END AS discount_bucket, o.customer_id, COUNT(*) AS row_count
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
GROUP BY CASE WHEN o.discount >= 0.44 THEN 'high' WHEN o.discount >= 0.12 THEN 'mid' ELSE 'low' END, o.customer_id
ORDER BY row_count DESC, discount_bucket, customer_id
LIMIT 10;
