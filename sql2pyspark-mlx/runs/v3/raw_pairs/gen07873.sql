SELECT YEAR(hire_date) AS hire_year, SORT_ARRAY(COLLECT_SET(level)) AS level_list, COUNT(*) AS row_count FROM employees GROUP BY hire_year HAVING row_count < 1
