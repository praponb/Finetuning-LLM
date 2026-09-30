SELECT location, COUNT(*) AS row_count FROM departments WHERE dept_name = 'Legal' AND location IN ('London', 'New York', 'Chiang Mai') GROUP BY location ORDER BY location
