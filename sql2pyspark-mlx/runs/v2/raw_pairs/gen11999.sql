SELECT country, SORT_ARRAY(COLLECT_SET(city)) AS city_list, COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count FROM suppliers GROUP BY country ORDER BY th_count DESC;
