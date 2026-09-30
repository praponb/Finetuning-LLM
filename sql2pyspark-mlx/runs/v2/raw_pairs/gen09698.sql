SELECT country, city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values, COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count FROM suppliers GROUP BY 1, 2;
