SELECT is_remote, DATE_TRUNC('MONTH', hire_date) AS hire_month, SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM employees
GROUP BY is_remote, hire_month;
