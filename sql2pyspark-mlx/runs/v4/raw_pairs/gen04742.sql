SELECT location, unique_dept_names
FROM (
    SELECT location, COUNT(DISTINCT budget) AS unique_dept_names
    FROM departments
    GROUP BY location
) AS agg
WHERE unique_dept_names >= 4
