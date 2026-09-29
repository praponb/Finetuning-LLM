SELECT location, dept_name, COUNT(*) AS total_count FROM departments GROUP BY 1, 2 ORDER BY total_count DESC, dept_name
