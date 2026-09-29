SELECT dept_name, location, row_count
FROM (
    SELECT dept_name, location, COUNT(*) AS row_count
    FROM departments
    GROUP BY dept_name, location
) AS t
WHERE row_count > 7
