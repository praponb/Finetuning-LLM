SELECT * FROM (SELECT level, job_title, salary FROM employees WHERE YEAR(hire_date) >= 2023)
PIVOT (MAX(salary) FOR job_title IN ('Engineer', 'Analyst', 'Manager'))
ORDER BY level;
