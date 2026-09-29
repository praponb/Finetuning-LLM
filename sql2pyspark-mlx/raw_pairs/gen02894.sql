SELECT CASE WHEN p.cost >= 700 THEN 'high' WHEN p.cost >= 110 THEN 'mid' ELSE 'low' END AS cost_bucket, p.category, MIN(i.quantity) AS min_quantity
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON s.id = i.store_id
GROUP BY CASE WHEN p.cost >= 700 THEN 'high' WHEN p.cost >= 110 THEN 'mid' ELSE 'low' END, p.category;
