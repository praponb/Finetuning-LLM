SELECT hire_date AS event_date
FROM employees
EXCEPT
SELECT last_restock AS event_date
FROM inventory
WHERE store_id IN (3, 4, 5, 6)
ORDER BY event_date
