SELECT CASE WHEN city IN ('New York', 'Singapore') THEN 'priority' ELSE 'other' END AS city_group, SORT_ARRAY(COLLECT_SET(city)) AS city_list, COUNT(DISTINCT rating) AS unique_countrys
FROM suppliers
GROUP BY CASE WHEN city IN ('New York', 'Singapore') THEN 'priority' ELSE 'other' END;
