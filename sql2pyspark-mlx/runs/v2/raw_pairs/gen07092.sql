SELECT QUARTER(signup_date) AS signup_quarter, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM customers GROUP BY signup_quarter ORDER BY signup_quarter DESC LIMIT 20;
