SELECT city, COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
FROM stores
WHERE store_name LIKE 'Ri%' AND opened_date BETWEEN '2023-03-20' AND '2024-03-01'
GROUP BY city
ORDER BY bangkok_count DESC;
