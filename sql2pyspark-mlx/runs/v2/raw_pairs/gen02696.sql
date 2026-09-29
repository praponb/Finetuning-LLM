SELECT dept_id, hire_date, level, DATE_SUB(hire_date, 3) AS hire_date_minus_3d, COALESCE(full_name, 'unassigned') AS full_name_filled FROM employees ORDER BY 1;
