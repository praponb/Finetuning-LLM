SELECT last_restock
FROM inventory
EXCEPT
SELECT hire_date last_restock
FROM employees
