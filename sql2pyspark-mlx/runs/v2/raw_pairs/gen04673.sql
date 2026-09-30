SELECT
    id,
    budget,
    dept_name,
    FIRST_VALUE(dept_name) OVER (ORDER BY budget, id) AS first_dept_name
FROM departments
WHERE budget < 2588000;
