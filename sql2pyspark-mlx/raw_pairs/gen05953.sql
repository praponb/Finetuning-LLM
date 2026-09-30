SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values FROM employees WHERE hire_date BETWEEN '2024-01-07' AND '2025-08-01'
