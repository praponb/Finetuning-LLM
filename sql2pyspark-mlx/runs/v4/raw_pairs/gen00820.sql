SELECT
    id,
    dept_name,
    budget,
    location
FROM (
    SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS rnk
    FROM departments
) AS r
WHERE rnk <= 5
