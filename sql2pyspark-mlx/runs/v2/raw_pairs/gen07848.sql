SELECT is_remote, COUNT(CASE WHEN level = 'L4' THEN 1 END) AS l4_count, MAX(salary) AS max_salary
FROM employees
WHERE job_title IN ('Engineer', 'Manager', 'Analyst')
GROUP BY is_remote;
