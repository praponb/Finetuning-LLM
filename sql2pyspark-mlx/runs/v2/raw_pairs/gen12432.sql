SELECT full_name
FROM employees
WHERE level = 'L2'
UNION ALL
SELECT full_name
FROM employees
WHERE UPPER(full_name) LIKE '%AVI%';
