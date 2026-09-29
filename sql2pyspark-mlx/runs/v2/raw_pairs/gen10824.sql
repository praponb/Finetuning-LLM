SELECT is_remote, SUM(salary) AS total_salary, SUM(SUM(salary)) OVER (ORDER BY is_remote) AS cumulative_salary FROM employees GROUP BY is_remote ORDER BY total_salary
