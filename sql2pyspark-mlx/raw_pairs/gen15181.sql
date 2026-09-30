SELECT s.region, COUNT(*) AS num_store_sales
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
LEFT OUTER JOIN products p ON ss.product_id = p.id
GROUP BY s.region
HAVING COUNT(*) > 1
ORDER BY 2, s.region;
