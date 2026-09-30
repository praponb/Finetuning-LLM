SELECT YEAR(hire_date) AS hire_year, SUM(salary) AS total_salary FROM employees GROUP BY YEAR(hire_date);
