SELECT DISTINCT job_title, level FROM employees WHERE job_title IN ('Director', 'Engineer', 'Designer') ORDER BY level, job_title LIMIT 5
