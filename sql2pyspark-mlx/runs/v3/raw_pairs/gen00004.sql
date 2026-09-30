SELECT level, dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values, MIN(hire_date) min_hire_date FROM employees GROUP BY level, dept_id
