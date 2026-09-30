SELECT region, city, SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) singapore_count
FROM stores
GROUP BY 1, 2;
