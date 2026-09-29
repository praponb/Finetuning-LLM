SELECT
    id,
    budget,
    LENGTH(location) AS location_length,
    CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END AS is_engineering
FROM departments
WHERE dept_name IN ('Finance', 'Engineering') AND budget < 2160000
