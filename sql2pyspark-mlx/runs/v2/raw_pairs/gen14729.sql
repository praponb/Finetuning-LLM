SELECT location
FROM departments
WHERE budget >= 3381000
UNION
SELECT location
FROM departments
WHERE budget >= 3683000
