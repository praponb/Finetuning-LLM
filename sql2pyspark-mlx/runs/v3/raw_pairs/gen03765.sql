SELECT hire_date FROM employees WHERE dept_id IN (4, 38) INTERSECT SELECT hire_date FROM employees WHERE job_title = 'Engineer';
