SELECT id, hire_date, manager_id, salary, COALESCE(job_title, 'unassigned') job_title_filled FROM employees
