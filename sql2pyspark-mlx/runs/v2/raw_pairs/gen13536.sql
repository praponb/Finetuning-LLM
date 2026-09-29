SELECT id, full_name FROM employees WHERE LOWER(email) LIKE '%any%' INTERSECT SELECT id, full_name FROM employees WHERE dept_id IN (10, 12, 28, 39) ORDER BY id;
