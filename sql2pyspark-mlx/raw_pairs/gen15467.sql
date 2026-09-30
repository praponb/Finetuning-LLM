SELECT level, SUM(salary) total_salary, COUNT(*) cnt, SUM(SUM(salary)) OVER (ORDER BY level) AS cumulative_salary FROM employees WHERE YEAR(hire_date) >= 2025 GROUP BY level ORDER BY level;
