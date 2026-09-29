SELECT SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END) AS finance_count FROM departments ORDER BY 1 DESC;
