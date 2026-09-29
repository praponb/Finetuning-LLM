SELECT city, COUNT(rating) AS rating_count, SORT_ARRAY(COLLECT_SET(city)) AS city_list, ROUND(AVG(rating), 2) AS avg_rating FROM suppliers GROUP BY city;
