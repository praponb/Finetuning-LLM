SELECT ROUND(AVG(salary), 2) AS avg_salary, SORT_ARRAY(COLLECT_SET(level)) AS level_list FROM employees WHERE job_title IN ('Intern', 'Director', 'Designer') AND email LIKE '%@company.co'
