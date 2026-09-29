SELECT s.city, COUNT(DISTINCT ss.revenue) unique_stores
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id
WHERE s.sqft BETWEEN 8239 AND 10201
GROUP BY s.city
HAVING unique_stores > 1;
