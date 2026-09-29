SELECT id, hire_date, dept_id FROM employees WHERE hire_date <= (SELECT MIN(hire_date) FROM employees WHERE full_name LIKE '%Lee%') ORDER BY dept_id DESC, id LIMIT 3;
