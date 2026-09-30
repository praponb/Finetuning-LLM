SELECT
    id,
    budget,
    dept_name,
    location
FROM departments
WHERE budget >= (SELECT AVG(budget) FROM departments)
ORDER BY budget;
