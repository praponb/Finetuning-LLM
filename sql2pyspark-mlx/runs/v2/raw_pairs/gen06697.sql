SELECT location, dept_name, COUNT(CASE WHEN location = 'London' THEN 1 END) AS london_count
FROM departments
GROUP BY location, dept_name
HAVING COUNT(CASE WHEN location = 'London' THEN 1 END) >= 5
