SELECT CASE WHEN city IN ('Tokyo', 'Bangkok') THEN 'tier_1' ELSE 'other' END AS city_group, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
WHERE store_name LIKE 'Sia%' AND opened_date > '2025-09-10'
GROUP BY CASE WHEN city IN ('Tokyo', 'Bangkok') THEN 'tier_1' ELSE 'other' END
