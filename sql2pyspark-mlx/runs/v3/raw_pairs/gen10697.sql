SELECT
    id,
    location,
    budget,
    ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rn
FROM departments
WHERE dept_name NOT IN ('Engineering', 'Finance');
