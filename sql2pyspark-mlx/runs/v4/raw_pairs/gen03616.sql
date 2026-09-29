SELECT *
FROM departments
WHERE budget <= 3943000 AND (budget < 3651000 OR location = 'Bangkok')
ORDER BY dept_name DESC, id ASC
LIMIT 10
