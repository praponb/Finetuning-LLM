SELECT p.brand, CASE WHEN p.cost >= 770 THEN 'high' WHEN p.cost >= 100 THEN 'mid' ELSE 'low' END AS cost_bucket, MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE p.category = 'Fashion'
GROUP BY p.brand, CASE WHEN p.cost >= 770 THEN 'high' WHEN p.cost >= 100 THEN 'mid' ELSE 'low' END
ORDER BY 2 DESC, brand
