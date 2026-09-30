SELECT YEAR(hire_date) hire_year, AVG(salary) avg_salary FROM employees GROUP BY YEAR(hire_date) ORDER BY hire_year DESC, avg_salary DESC;
