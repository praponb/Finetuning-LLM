SELECT is_remote, SUM(salary) total_salary, COUNT(*) cnt, DENSE_RANK() OVER (ORDER BY SUM(salary) DESC) AS salary_rank FROM employees GROUP BY is_remote ORDER BY total_salary DESC;
