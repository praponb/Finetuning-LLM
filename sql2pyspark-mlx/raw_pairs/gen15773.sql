SELECT *
FROM (
    SELECT
        location,
        dept_name,
        budget
    FROM departments
) src
PIVOT (
    MAX(budget)
    FOR dept_name IN ('HR' AS hr, 'Sales' AS sales, 'Engineering' AS engineering)
);
