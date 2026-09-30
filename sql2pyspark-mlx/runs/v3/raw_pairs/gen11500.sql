SELECT QUARTER(signup_date) AS signup_quarter, MIN(age) AS min_age, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM customers GROUP BY 1
