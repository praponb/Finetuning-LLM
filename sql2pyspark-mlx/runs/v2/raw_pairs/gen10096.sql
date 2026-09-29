SELECT *, ROUND(salary, 2) AS salary_rounded
FROM employees
WHERE full_name LIKE '%Car%'
ORDER BY level, id
LIMIT 5
