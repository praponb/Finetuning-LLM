SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values, COUNT(*) AS cnt, MAX(rating) - MIN(rating) AS rating_range FROM suppliers ORDER BY rating_range DESC
