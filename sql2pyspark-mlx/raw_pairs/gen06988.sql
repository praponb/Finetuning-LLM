SELECT level, COUNT(DISTINCT salary) AS unique_job_titles, MIN(salary) AS min_salary
FROM employees
WHERE salary >= 78000 OR is_remote
GROUP BY 1
ORDER BY unique_job_titles DESC, min_salary DESC
