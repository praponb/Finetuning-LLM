SELECT CASE WHEN s.city IN ('Chiang Mai', 'Tokyo') THEN 'tier_1' ELSE 'other' END AS city_group, s.city, COUNT(*) AS cnt
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE i.store_id = 3
GROUP BY CASE WHEN s.city IN ('Chiang Mai', 'Tokyo') THEN 'tier_1' ELSE 'other' END, s.city
ORDER BY city_group
