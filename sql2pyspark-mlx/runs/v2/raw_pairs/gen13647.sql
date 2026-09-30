SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
) AS ranked
WHERE row_num <= 1
