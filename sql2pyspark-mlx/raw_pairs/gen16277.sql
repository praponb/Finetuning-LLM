SELECT manager_id FROM employees INTERSECT SELECT manager_id FROM employees WHERE UPPER(TRIM(email)) = 'IVY@GMAIL.COM'
