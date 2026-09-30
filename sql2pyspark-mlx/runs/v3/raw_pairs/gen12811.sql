SELECT is_active, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, AVG(credit_limit) AS avg_credit_limit FROM customers GROUP BY is_active ORDER BY avg_credit_limit ASC;
