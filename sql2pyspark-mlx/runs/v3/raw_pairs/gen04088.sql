WITH numbered AS (
    SELECT
        id,
        location,
        budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rnk
    FROM departments
    WHERE location IN ('Berlin', 'Tokyo', 'Singapore')
)
SELECT *
FROM numbered
WHERE rnk <= 3;
