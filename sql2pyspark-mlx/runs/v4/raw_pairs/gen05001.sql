WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS row_num
    FROM departments
)
SELECT *
FROM numbered
WHERE row_num <= 2;
