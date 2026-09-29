SELECT id, location, budget
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
    WHERE location = 'Berlin'
) AS t
WHERE row_num = 1
