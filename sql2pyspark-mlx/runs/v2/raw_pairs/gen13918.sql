SELECT e.id, e.salary, e.is_remote, e.job_title FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE budget <= 2304000) ORDER BY id DESC, salary;
