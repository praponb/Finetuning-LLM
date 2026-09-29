SELECT last_restock, 'inventory' AS source FROM inventory UNION SELECT hire_date AS last_restock, 'employee' AS source FROM employees;
