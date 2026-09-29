SELECT hire_date AS event_date
FROM employees
WHERE dept_id IN (1, 15, 20)
UNION
SELECT last_restock AS event_date
FROM inventory
WHERE store_id IN (1, 2, 4, 5)
