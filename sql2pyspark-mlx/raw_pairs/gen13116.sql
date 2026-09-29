SELECT YEAR(hire_date) AS hire_year, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list FROM employees GROUP BY 1 HAVING COUNT(*) < 2 ORDER BY hire_year DESC LIMIT 20;
