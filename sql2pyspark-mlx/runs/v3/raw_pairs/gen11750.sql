SELECT region, CASE WHEN city IN ('Chiang Mai', 'Singapore') THEN 'Group A' ELSE 'Other' END AS city_group
FROM stores
WHERE region IN ('East', 'Central', 'North')
ORDER BY city_group ASC
