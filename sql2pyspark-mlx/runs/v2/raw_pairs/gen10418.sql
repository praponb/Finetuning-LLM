SELECT id, dept_id, INITCAP(TRIM(email)) AS email_title FROM employees WHERE hire_date > '2024-05-20' AND dept_id = 22;
