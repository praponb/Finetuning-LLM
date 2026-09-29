SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values, MAX(hire_date) last_hire_date FROM employees ORDER BY last_hire_date;
