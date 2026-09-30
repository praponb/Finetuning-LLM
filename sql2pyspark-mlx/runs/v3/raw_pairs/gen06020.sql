SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values, COUNT(DISTINCT salary) AS unique_depts FROM employees GROUP BY dept_id
