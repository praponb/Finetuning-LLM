SELECT country, COUNT(*) AS num_customers, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM customers GROUP BY 1
