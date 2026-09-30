SELECT country, city, cnt, city_list FROM (SELECT country, city, COUNT(*) AS cnt, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM suppliers GROUP BY country, city) AS t WHERE cnt < 4;
