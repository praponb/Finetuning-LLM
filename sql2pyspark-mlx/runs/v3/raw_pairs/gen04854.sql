SELECT level, MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE full_name LIKE '%Car%'
GROUP BY level
HAVING salary_range < 73
