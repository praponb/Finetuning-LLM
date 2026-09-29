SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values, MIN(salary) min_salary, ROUND(AVG(salary), 2) avg_salary
FROM employees
WHERE manager_id IS NOT NULL;
