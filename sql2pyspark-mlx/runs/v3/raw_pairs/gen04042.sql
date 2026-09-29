SELECT QUARTER(signup_date) AS signup_quarter, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM customers
WHERE city IN ('Singapore', 'New York', 'Chiang Mai')
GROUP BY QUARTER(signup_date);
