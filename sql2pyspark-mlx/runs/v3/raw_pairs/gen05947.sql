SELECT SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END) AS engineer_count, MIN(salary) AS min_salary
FROM employees
ORDER BY min_salary DESC
