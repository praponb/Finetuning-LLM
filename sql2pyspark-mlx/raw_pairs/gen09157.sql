SELECT id, manager_id, CONCAT(level, ', ', full_name) AS level_full_name, ROUND(salary, 2) AS salary_rounded FROM employees WHERE full_name LIKE '%FRANK%' AND salary >= 86000;
