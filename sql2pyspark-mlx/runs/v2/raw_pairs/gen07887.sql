SELECT level, SORT_ARRAY(COLLECT_SET(level)) AS level_list FROM employees WHERE job_title = 'Manager' AND full_name LIKE '%Mia%' AND hire_date BETWEEN '2024-11-01' AND '2025-08-01' GROUP BY 1;
