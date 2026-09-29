SELECT manager_id, SUM(salary) AS total_salary, SUM(SUM(salary)) OVER (ORDER BY manager_id) AS cumulative_salary FROM employees GROUP BY manager_id ORDER BY manager_id LIMIT 1
