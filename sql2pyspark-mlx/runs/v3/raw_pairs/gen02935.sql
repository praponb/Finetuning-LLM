SELECT
    id,
    location,
    budget,
    CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai
FROM departments
WHERE dept_name = 'Finance' OR budget < 2719000
ORDER BY is_chiang_mai, id
LIMIT 50;
