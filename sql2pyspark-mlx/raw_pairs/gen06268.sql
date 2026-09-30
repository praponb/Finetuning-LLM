SELECT dept_name, location
FROM departments
WHERE location NOT IN ('Berlin', 'Bangkok', 'London')
UNION
SELECT dept_name, location
FROM departments
WHERE budget < 1659000
ORDER BY location;
