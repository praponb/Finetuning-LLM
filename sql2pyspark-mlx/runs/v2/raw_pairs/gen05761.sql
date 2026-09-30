WITH top_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
    WHERE location = 'London'
)
SELECT *
FROM top_departments
WHERE row_num <= 3
ORDER BY id DESC, location DESC;
