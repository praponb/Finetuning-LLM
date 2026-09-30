SELECT d.budget, d.dept_name, t.avg_salary FROM departments d LEFT JOIN (SELECT dept_id, AVG(salary) AS avg_salary FROM employees GROUP BY dept_id) t ON d.id = t.dept_id;
