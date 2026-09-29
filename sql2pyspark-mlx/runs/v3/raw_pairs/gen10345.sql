SELECT CASE WHEN city IN ('Chiang Mai', 'Singapore') THEN 'tier_1' ELSE 'other' END AS city_group, ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
GROUP BY CASE WHEN city IN ('Chiang Mai', 'Singapore') THEN 'tier_1' ELSE 'other' END
HAVING ROUND(STDDEV(rating), 2) < 78;
