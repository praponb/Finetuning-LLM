SELECT manager_id, full_name, SUBSTRING(full_name, 1, 3) AS full_name_prefix FROM employees WHERE manager_id IN (4, 22) ORDER BY full_name, full_name_prefix;
