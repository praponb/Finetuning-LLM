SELECT s.id, s.city, s.store_name
FROM stores s
WHERE s.city = 'Tokyo' AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.product_id IN (7, 10))
ORDER BY 2 DESC
