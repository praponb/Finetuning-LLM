SELECT s.id, s.region
FROM stores s
WHERE s.city = 'Tokyo' AND s.id IN (SELECT store_id FROM store_sales WHERE store_id IN (1, 3, 5, 6))
ORDER BY 2;
