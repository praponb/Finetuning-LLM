SELECT QUARTER(hire_date) AS hire_quarter, SUM(salary) AS total_salary FROM employees GROUP BY hire_quarter HAVING SUM(salary) < 663000 ORDER BY total_salary DESC
