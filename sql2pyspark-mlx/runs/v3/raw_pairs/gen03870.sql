SELECT city, COUNT(CASE WHEN city = 'London' THEN 1 END) london_count
FROM stores
GROUP BY city;
