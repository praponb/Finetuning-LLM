SELECT city, segment, SUM(age) AS total_age, SUM(credit_limit) AS total_credit_limit, SORT_ARRAY(COLLECT_SET(country)) AS country_list FROM customers GROUP BY city, segment HAVING total_age > 140
