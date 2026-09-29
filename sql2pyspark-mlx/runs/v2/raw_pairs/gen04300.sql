SELECT
    id,
    budget,
    dept_name,
    location
FROM departments
WHERE budget > (SELECT MAX(budget) FROM departments)
ORDER BY 2 DESC, 1 DESC
