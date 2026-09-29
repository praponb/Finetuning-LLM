SELECT ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit, SORT_ARRAY(COLLECT_SET(segment)) AS segment_list FROM customers WHERE email LIKE '%@outlook.com' OR is_active = TRUE ORDER BY 1 DESC;
