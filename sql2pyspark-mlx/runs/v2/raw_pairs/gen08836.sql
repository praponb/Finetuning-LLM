SELECT level, AVG(salary) AS avg_salary FROM employees GROUP BY level HAVING AVG(salary) < 128000;
