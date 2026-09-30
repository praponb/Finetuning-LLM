SELECT COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count
FROM departments
WHERE budget > 2533000
