WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC) rn
    FROM departments
    WHERE budget BETWEEN 1420000 AND 2308000
)
SELECT id, location, budget
FROM numbered
WHERE rn = 1
ORDER BY location;
