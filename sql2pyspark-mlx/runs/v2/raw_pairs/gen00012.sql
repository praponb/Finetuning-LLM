SELECT id, full_name, level, COALESCE(email, 'unassigned') AS email_filled FROM employees WHERE UPPER(full_name) LIKE '%RAC%' AND dept_id IN (7, 9, 21, 40) AND manager_id IS NULL;
