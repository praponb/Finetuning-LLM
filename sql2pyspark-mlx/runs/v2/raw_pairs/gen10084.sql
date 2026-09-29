SELECT
    id,
    budget,
    location,
    dept_name
FROM departments
WHERE budget BETWEEN 1092000 AND 3271000 AND dept_name != 'Legal';
