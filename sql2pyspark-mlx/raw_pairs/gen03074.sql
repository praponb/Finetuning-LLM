SELECT * FROM (SELECT QUARTER(hire_date) AS hire_quarter, job_title, salary FROM employees)
PIVOT (MAX(salary) FOR job_title IN ('Engineer', 'Manager', 'Intern'))
ORDER BY hire_quarter;
