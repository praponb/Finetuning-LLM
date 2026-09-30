SELECT *
FROM (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS rnk
    FROM departments
) AS t
WHERE rnk <= 1
