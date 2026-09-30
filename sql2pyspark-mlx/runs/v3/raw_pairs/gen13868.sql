SELECT ROUND((100 * SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_analyst FROM employees WHERE UPPER(email) LIKE '%ARO%' AND salary >= 58000
