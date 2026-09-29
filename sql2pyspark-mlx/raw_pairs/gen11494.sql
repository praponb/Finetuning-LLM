SELECT
    id,
    location,
    budget,
    dept_name
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rnk
    FROM departments
) AS r
WHERE rnk <= 3
ORDER BY budget;
