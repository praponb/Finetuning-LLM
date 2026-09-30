SELECT s.id, s.store_name, COUNT(ss.id) AS store_sale_count
FROM stores s
JOIN store_sales ss ON ss.store_id = s.id
WHERE ss.sale_date < '2023-12-22'
GROUP BY s.id, s.store_name
HAVING COUNT(ss.id) > 5
ORDER BY store_name ASC
