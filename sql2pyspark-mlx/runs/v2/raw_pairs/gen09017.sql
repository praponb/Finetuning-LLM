SELECT level, SUM(salary) total_salary, RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk FROM employees GROUP BY level ORDER BY rnk DESC
