SELECT job_title, SUM(salary) AS total_salary, RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk FROM employees GROUP BY job_title;
