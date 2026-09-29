SELECT
    id,
    location,
    CONCAT(dept_name, '/', location) AS dept_name_location,
    CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai
FROM departments
