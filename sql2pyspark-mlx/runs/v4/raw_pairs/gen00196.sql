SELECT MAX(signup_date) AS latest_signup_date, SUM(CASE WHEN segment = 'Consumer' THEN credit_limit ELSE 0 END) AS consumer_credit_limit, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM customers;
