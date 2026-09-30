SELECT
    CASE WHEN city IN ('Singapore', 'New York') THEN 'tier_1' ELSE 'other' END AS city_group,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    ROUND((100 * SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_th
FROM suppliers
WHERE rating < 4.0 AND rating BETWEEN 2.3 AND 3.8 AND rating BETWEEN 3.5 AND 3.9
GROUP BY 1
ORDER BY city_group ASC;
