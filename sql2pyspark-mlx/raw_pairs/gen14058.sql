SELECT *
FROM (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rnk
    FROM departments
    WHERE budget < 3600000
) AS ranked
WHERE rnk <= 3
ORDER BY 1;
