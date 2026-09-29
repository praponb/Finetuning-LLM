WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) pos
    FROM departments
)
SELECT *
FROM numbered
WHERE pos <= 3;
