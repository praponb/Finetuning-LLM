SELECT SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END) AS legal_count, COUNT(*) AS row_count FROM departments ORDER BY row_count, legal_count
