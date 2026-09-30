SELECT CASE WHEN p.price >= 1230 THEN 'high' WHEN p.price >= 600 THEN 'mid' ELSE 'low' END AS price_bucket, AVG(i.quantity) AS avg_quantity, COUNT(i.quantity) AS quantity_count
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
JOIN stores s ON i.store_id = s.id
GROUP BY CASE WHEN p.price >= 1230 THEN 'high' WHEN p.price >= 600 THEN 'mid' ELSE 'low' END
HAVING AVG(i.quantity) >= 90
ORDER BY quantity_count DESC, price_bucket
LIMIT 20;
