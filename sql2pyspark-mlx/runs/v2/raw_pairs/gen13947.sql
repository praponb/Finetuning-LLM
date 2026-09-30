SELECT id, hire_date, full_name, INITCAP(TRIM(level)) AS level_title FROM employees WHERE full_name LIKE '%Rossi%' ORDER BY hire_date ASC;
