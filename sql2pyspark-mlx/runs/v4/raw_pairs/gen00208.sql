SELECT is_remote, SORT_ARRAY(COLLECT_SET(level)) AS level_list, MIN(hire_date) AS first_hire_date
FROM employees
WHERE email IS NOT NULL
GROUP BY 1
ORDER BY first_hire_date DESC;
