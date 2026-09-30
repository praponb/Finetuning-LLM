SELECT level, job_title, ROUND(STDDEV(salary), 2) AS stddev_salary, MAX(hire_date) AS latest_hire_date FROM employees GROUP BY level, job_title HAVING MAX(salary) < 138000 ORDER BY level ASC
