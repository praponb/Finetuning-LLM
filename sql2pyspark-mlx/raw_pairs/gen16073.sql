SELECT SUM(rating) total_rating, SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END) chiang_mai_count
FROM suppliers
ORDER BY chiang_mai_count
