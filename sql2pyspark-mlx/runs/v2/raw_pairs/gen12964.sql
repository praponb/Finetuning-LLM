SELECT dept_name
FROM departments
WHERE location IS NOT NULL
INTERSECT
SELECT dept_name
FROM departments
WHERE budget > 1752000;
