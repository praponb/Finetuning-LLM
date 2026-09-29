SELECT s.city, COUNT(*) AS row_count
FROM store_sales ss
INNER JOIN stores s ON s.id = ss.store_id
LEFT OUTER JOIN products p ON ss.product_id = p.id
GROUP BY s.city
HAVING COUNT(*) > 1
ORDER BY row_count DESC
