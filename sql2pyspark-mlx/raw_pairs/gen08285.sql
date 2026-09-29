SELECT level, SUM(salary) AS total_salary, COUNT(*) AS cnt, SUM(SUM(salary)) OVER (ORDER BY level) AS cumulative_salary FROM employees GROUP BY level;
