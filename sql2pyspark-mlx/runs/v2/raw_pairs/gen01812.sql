SELECT id, dept_id, hire_date FROM employees WHERE email LIKE '%@mail.co.th' AND job_title IN ('Intern', 'Manager') ORDER BY dept_id DESC, hire_date DESC, id LIMIT 50
