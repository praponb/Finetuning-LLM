SELECT
    id,
    location,
    budget,
    dept_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos
    FROM departments
) AS r
WHERE pos <= 3
ORDER BY location DESC
