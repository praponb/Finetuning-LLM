SELECT city, city_group, uk_count
FROM (
    SELECT city, CASE WHEN city IN ('Singapore', 'Chiang Mai') THEN 'core' ELSE 'other' END AS city_group, COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
    FROM suppliers
    GROUP BY city, CASE WHEN city IN ('Singapore', 'Chiang Mai') THEN 'core' ELSE 'other' END
) AS x
WHERE uk_count >= 4
ORDER BY city_group, uk_count;
