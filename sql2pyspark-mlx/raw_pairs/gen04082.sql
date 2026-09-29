SELECT *, CONCAT(full_name, ' - ', level) AS full_name_level FROM employees WHERE dept_id IN (6, 8, 13) AND dept_id IN (1, 5);
