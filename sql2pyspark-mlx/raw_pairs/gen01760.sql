SELECT dept_name, COUNT(DISTINCT budget) AS unique_dept_names, COUNT(*) AS row_count
FROM departments
WHERE location = 'Bangkok' OR dept_name IN ('Engineering', 'Finance', 'Sales')
GROUP BY dept_name
HAVING row_count >= 5;
