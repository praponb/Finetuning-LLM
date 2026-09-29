SELECT id, is_remote, salary, manager_id, CASE WHEN salary >= 135000 THEN 'High' WHEN salary >= 69000 THEN 'Medium' ELSE 'Low' END salary_tier FROM employees ORDER BY manager_id, id LIMIT 5
