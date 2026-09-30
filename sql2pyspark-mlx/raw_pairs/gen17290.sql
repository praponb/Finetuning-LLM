SELECT YEAR(hire_date) AS hire_year, MONTH(hire_date) AS hire_month, ROUND(STDDEV(salary), 2) AS stddev_salary FROM employees GROUP BY YEAR(hire_date), MONTH(hire_date) ORDER BY hire_year DESC;
