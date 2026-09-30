SELECT
    id,
    dept_name,
    budget,
    location,
    LAST_VALUE(location) OVER (PARTITION BY dept_name ORDER BY budget DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_location
FROM departments
WHERE location != 'New York';
