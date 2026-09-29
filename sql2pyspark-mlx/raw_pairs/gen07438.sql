SELECT email, manager_id
FROM employees
WHERE salary BETWEEN 81000 AND 98000
INTERSECT
SELECT email, manager_id
FROM employees
WHERE full_name IS NOT NULL
ORDER BY email;
