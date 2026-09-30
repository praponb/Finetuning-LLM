SELECT location, SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) AS legal_budget FROM departments GROUP BY 1 ORDER BY location
