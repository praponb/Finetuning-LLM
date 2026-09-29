SELECT QUARTER(hire_date) AS hire_quarter, SUM(salary) AS total_salary, SUM(SUM(salary)) OVER (ORDER BY QUARTER(hire_date)) AS cumulative_salary FROM employees GROUP BY QUARTER(hire_date);
