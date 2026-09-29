SELECT id, location
FROM departments
WHERE budget < 3666000
INTERSECT
SELECT id, location
FROM departments
WHERE budget > 2584000
ORDER BY id
