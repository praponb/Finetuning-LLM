SELECT location, dept_name, COUNT(DISTINCT budget) AS unique_locations
FROM departments
WHERE dept_name IN ('Sales', 'Finance') AND budget > 3555000 AND location IN ('New York', 'Berlin')
GROUP BY location, dept_name
ORDER BY dept_name DESC
