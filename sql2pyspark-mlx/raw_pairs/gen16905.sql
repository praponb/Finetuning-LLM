SELECT store_name, CASE WHEN city IN ('Bangkok', 'Singapore') THEN 'Core' ELSE 'Other' END AS city_group
FROM stores
ORDER BY city_group
