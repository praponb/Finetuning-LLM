SELECT location
FROM departments
WHERE dept_name = 'Legal' AND dept_name IN ('Sales', 'Legal', 'Finance')
ORDER BY location
