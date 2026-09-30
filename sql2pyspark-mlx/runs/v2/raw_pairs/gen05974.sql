SELECT id, budget, ROW_NUMBER() OVER (ORDER BY budget, id) AS rn
FROM departments
WHERE location = 'Berlin'
