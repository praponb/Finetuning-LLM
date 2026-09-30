SELECT CASE WHEN region IN ('North', 'Central') THEN 'tier_1' ELSE 'other' END AS region_group, city, COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count
FROM stores
WHERE city NOT IN ('Bangkok', 'London')
GROUP BY 1, 2
HAVING COUNT(CASE WHEN region = 'Central' THEN 1 END) < 1
