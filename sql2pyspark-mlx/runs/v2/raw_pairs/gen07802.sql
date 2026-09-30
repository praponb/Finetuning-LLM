SELECT location, COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count FROM departments GROUP BY location ORDER BY location;
