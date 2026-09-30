SELECT DATE_FORMAT(customers.signup_date, 'yyyy-MM') AS signup_month, SORT_ARRAY(COLLECT_SET(sessions.device)) AS device_list, SORT_ARRAY(COLLECT_SET(sessions.country)) AS country_list
FROM sessions
FULL OUTER JOIN customers ON sessions.customer_id = customers.id
GROUP BY DATE_FORMAT(customers.signup_date, 'yyyy-MM')
ORDER BY signup_month DESC
