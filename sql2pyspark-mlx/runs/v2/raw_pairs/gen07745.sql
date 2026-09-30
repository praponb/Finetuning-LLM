SELECT departments.id, departments.budget, departments.dept_name FROM departments WHERE departments.id IN (SELECT dept_id FROM employees WHERE job_title = 'Designer');
