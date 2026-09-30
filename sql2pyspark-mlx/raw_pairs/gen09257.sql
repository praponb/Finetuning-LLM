SELECT city, MAX(rating) - MIN(rating) rating_range, SORT_ARRAY(COLLECT_SET(country)) country_list FROM suppliers WHERE city IN ('Bangkok', 'New York', 'Berlin') AND rating > 4.1 GROUP BY city
