SELECT is_remote, COUNT(DISTINCT salary) AS unique_job_titles
FROM employees
GROUP BY is_remote
ORDER BY unique_job_titles DESC;
