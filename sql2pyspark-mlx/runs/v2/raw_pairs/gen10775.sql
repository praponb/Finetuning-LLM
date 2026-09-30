SELECT *
FROM departments
WHERE dept_name NOT IN ('Engineering', 'Marketing') AND budget IS NULL;
