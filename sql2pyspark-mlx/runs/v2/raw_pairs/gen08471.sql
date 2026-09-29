SELECT
    id,
    budget,
    dept_name,
    location
FROM departments
WHERE budget >= (SELECT MAX(budget) FROM departments)
ORDER BY dept_name;
