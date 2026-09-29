SELECT *
FROM (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos
    FROM departments
) AS t
WHERE pos <= 3
ORDER BY id
