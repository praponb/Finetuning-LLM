SELECT id, full_name, hire_date, SPLIT(LOWER(email), '@')[0] email_user FROM employees WHERE dept_id IN (31, 32, 37, 38) AND manager_id IN (1, 32) ORDER BY full_name;
