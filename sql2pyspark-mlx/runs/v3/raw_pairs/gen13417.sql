SELECT * FROM (SELECT dept_id, is_remote, ROUND(AVG(salary), 2) AS avg_salary FROM employees WHERE salary BETWEEN 60000 AND 105000 GROUP BY dept_id, is_remote) AS s WHERE avg_salary >= 73
