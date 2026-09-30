SELECT s.region, COUNT(DISTINCT ss.units) AS unique_products
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
GROUP BY s.region
HAVING COUNT(DISTINCT ss.units) >= 4
