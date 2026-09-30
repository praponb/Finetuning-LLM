SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM departments
) src
PIVOT (
    MAX(budget) AS total,
    COUNT(1) AS cnt
    FOR location IN ('Bangkok' AS bangkok, 'Berlin' AS berlin, 'London' AS london)
)
