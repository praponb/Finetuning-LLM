SELECT COUNT(DISTINCT salary) unique_job_titles, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) l5_count, MAX(hire_date) last_hire_date
FROM employees
WHERE dept_id IN (8, 15, 20, 21) OR salary BETWEEN 77000 AND 82000
ORDER BY unique_job_titles, l5_count DESC;
