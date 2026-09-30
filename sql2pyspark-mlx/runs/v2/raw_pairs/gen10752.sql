SELECT * FROM (SELECT QUARTER(hire_date) AS hire_quarter, job_title, salary FROM employees)
PIVOT (AVG(salary) FOR job_title IN ('Analyst', 'Director', 'Manager'))
ORDER BY hire_quarter
