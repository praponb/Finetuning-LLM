SELECT YEAR(c.signup_date) AS signup_year, SORT_ARRAY(COLLECT_SET(s.browser)) AS browser_list, COUNT(CASE WHEN s.country = 'TH' THEN 1 END) AS th_count
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.browser IN ('Firefox', 'Edge')
GROUP BY YEAR(c.signup_date)
ORDER BY signup_year DESC;
