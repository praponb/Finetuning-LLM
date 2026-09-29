SELECT id, full_name, INITCAP(TRIM(full_name)) AS full_name_title FROM employees WHERE full_name LIKE 'Da%';
