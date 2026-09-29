SELECT QUARTER(signup_date) AS signup_quarter, SORT_ARRAY(COLLECT_SET(country)) AS country_list FROM customers GROUP BY QUARTER(signup_date) ORDER BY 1
