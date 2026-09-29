SELECT COUNT(DISTINCT salary) AS unique_job_titles
FROM employees
WHERE hire_date BETWEEN '2023-05-01' AND '2025-09-01' AND LOWER(full_name) LIKE '%any%'
ORDER BY unique_job_titles DESC;
