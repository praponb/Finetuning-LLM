SELECT SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count, SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END) AS jp_count, ROUND(STDDEV(rating), 2) AS stddev_rating FROM suppliers;
