SELECT s.opened_date, s.city, ss.revenue
FROM stores s
LEFT JOIN store_sales ss ON s.id = ss.store_id
WHERE ss.units BETWEEN 17 AND 24
ORDER BY revenue;
