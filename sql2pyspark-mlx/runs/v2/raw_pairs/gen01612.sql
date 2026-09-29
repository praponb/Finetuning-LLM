SELECT is_remote, SUM(salary) AS total_salary, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(salary) DESC) AS salary_rank FROM employees GROUP BY is_remote;
