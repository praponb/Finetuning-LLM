SELECT id, email, CASE WHEN level IN ('L4', 'L2') THEN 'Group A' ELSE 'Other' END AS level_group FROM employees WHERE dept_id IN (6, 14, 20, 40) ORDER BY email DESC;
