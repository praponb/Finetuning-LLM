SELECT
    id,
    budget,
    location,
    dept_name
FROM departments
WHERE budget >= (SELECT AVG(budget) FROM departments WHERE dept_name != 'Legal');
