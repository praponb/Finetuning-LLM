SELECT country, city, COUNT(*) total_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values FROM suppliers GROUP BY country, city ORDER BY country ASC;
