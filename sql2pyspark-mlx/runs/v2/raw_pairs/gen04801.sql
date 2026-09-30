SELECT dept_name, location
FROM departments
WHERE budget <= 3278000
UNION
SELECT dept_name, location
FROM departments
WHERE budget < 1838000
