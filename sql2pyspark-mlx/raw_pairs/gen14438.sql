SELECT dept_id, SUM(salary) AS total_salary, RANK() OVER (ORDER BY SUM(salary) DESC) AS salary_rank
FROM employees
GROUP BY dept_id
ORDER BY dept_id;
