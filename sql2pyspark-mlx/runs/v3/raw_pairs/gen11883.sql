SELECT * FROM (SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, job_title, salary FROM employees)
PIVOT (AVG(salary) FOR job_title IN ('Analyst', 'Manager', 'Director', 'Engineer'))
ORDER BY hire_cohort;
