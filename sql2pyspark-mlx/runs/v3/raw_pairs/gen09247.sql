SELECT *
FROM employees
WHERE NOT (hire_date > '2023-07-01' AND (level = 'L1' OR level IN ('L1', 'L2', 'L4')))
ORDER BY 1 DESC
LIMIT 10
