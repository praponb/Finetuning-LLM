SELECT
    id,
    dept_name,
    budget,
    SUBSTRING(dept_name, 1, 2) AS dept_name_prefix
FROM departments
WHERE budget BETWEEN 1372000 AND 2032000 AND location NOT IN ('Bangkok', 'Singapore');
