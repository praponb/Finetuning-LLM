SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM departments
) src
PIVOT (
    SUM(budget) AS amount
    FOR location IN ('Singapore' AS singapore, 'Chiang Mai' AS chiang_mai, 'Bangkok' AS bangkok)
);
