SELECT city, AVG(rating) avg_rating, SORT_ARRAY(COLLECT_SET(city)) city_list, COUNT(DISTINCT rating) unique_citys FROM suppliers WHERE rating >= 3.3 GROUP BY city;
