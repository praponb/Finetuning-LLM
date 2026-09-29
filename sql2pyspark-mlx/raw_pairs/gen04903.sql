SELECT hire_date
FROM employees
WHERE hire_date IS NOT NULL
UNION
SELECT last_restock
FROM inventory
WHERE last_restock IS NOT NULL
