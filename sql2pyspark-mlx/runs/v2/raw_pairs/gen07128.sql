SELECT level, SUM(salary) AS total_salary, RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
GROUP BY level
ORDER BY level DESC, rnk DESC;
