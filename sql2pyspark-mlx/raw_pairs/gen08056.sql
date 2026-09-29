SELECT id, manager_id FROM employees WHERE LOWER(full_name) LIKE '%lic%' UNION SELECT id, manager_id FROM employees WHERE LENGTH(email) > 10;
