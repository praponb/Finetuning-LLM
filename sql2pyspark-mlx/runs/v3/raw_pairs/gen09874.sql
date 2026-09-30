SELECT id, email, SUBSTRING(full_name, 1, 3) AS full_name_prefix, CONCAT(level, ' ', full_name) AS level_full_name FROM employees;
