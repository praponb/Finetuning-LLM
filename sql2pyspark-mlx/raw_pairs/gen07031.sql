SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM departments
) src
PIVOT (
    MAX(budget) AS amount
    FOR location IN ('Chiang Mai' AS chiang_mai, 'New York' AS new_york)
);
