SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values, MAX(salary) - MIN(salary) AS salary_range
FROM employees
GROUP BY 1;
