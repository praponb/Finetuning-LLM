SELECT
    id,
    dept_name,
    budget,
    location
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS row_num
    FROM departments
) AS ranked
WHERE row_num <= 2
ORDER BY 1 DESC
