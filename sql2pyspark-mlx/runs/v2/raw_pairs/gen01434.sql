SELECT id, hire_date, manager_id, DATE_SUB(hire_date, 72) AS hire_date_minus_72d FROM employees WHERE job_title IN ('Intern', 'Designer', 'Director') AND level = 'L5' ORDER BY id DESC LIMIT 50
