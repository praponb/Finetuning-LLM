SELECT id, budget
FROM departments
WHERE budget > (SELECT MAX(budget) FROM departments WHERE location IN ('Tokyo', 'Bangkok'))
