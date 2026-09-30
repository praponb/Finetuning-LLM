SELECT *
FROM (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
    FROM departments
    WHERE budget > 3174000
) AS t
WHERE rn <= 3
ORDER BY rn DESC, id DESC;
