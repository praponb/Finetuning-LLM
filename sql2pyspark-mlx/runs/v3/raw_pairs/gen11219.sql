SELECT MONTH(hire_date) AS hire_month, level, ROUND(AVG(salary), 2) AS avg_salary, SUM(salary) AS total_salary FROM employees GROUP BY MONTH(hire_date), level HAVING SUM(salary) > 614000;
