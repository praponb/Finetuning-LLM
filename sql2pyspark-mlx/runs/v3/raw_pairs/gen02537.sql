SELECT id, location, CONCAT(dept_name, ' - ', location) AS dept_name_location
FROM departments
WHERE (budget <= 2337000 AND dept_name IN ('Legal', 'Marketing', 'Sales')) OR budget BETWEEN 1201000 AND 2518000
