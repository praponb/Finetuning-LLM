SELECT hire_date
FROM employees
UNION
SELECT last_restock
FROM inventory
WHERE product_id IN (1, 2, 3, 7)
ORDER BY hire_date
