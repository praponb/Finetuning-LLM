SELECT CASE WHEN p.cost >= 720 THEN 'high' WHEN p.cost >= 250 THEN 'mid' ELSE 'low' END AS cost_bucket, COUNT(*) AS total_count
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
WHERE i.store_id = 3
GROUP BY CASE WHEN p.cost >= 720 THEN 'high' WHEN p.cost >= 250 THEN 'mid' ELSE 'low' END
ORDER BY cost_bucket;
