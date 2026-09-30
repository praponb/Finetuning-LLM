SELECT level, MIN(salary) AS min_salary, ROUND(STDDEV(salary), 2) AS stddev_salary FROM employees GROUP BY 1
