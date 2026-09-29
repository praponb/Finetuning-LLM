SELECT *
FROM departments
WHERE location IS NULL
ORDER BY budget, dept_name DESC;
