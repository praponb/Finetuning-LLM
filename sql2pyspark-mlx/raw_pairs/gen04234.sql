SELECT MONTH(c.signup_date) signup_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.country))) country_values, SORT_ARRAY(COLLECT_SET(s.browser)) browser_list
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IN (21, 22, 30, 33)
GROUP BY MONTH(c.signup_date)
