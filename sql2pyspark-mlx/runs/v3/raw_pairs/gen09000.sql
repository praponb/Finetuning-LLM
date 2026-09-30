SELECT * FROM (SELECT country, city, rating FROM suppliers)
PIVOT (SUM(rating) FOR city IN ('Singapore', 'Tokyo', 'Bangkok'))
ORDER BY country;
