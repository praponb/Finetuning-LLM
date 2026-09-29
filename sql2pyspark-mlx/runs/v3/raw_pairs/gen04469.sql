SELECT location, 'department' AS source
FROM departments
WHERE dept_name NOT IN ('Finance', 'Legal')
UNION ALL
SELECT city AS location, 'store' AS source
FROM stores
ORDER BY location;
