SELECT a.id, a.dept_id, a.salary, a.job_title FROM employees a WHERE a.salary > (SELECT AVG(b.salary) FROM employees b WHERE b.dept_id = a.dept_id);
