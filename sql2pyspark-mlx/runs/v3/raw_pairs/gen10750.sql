SELECT last_restock AS event_date
FROM inventory
WHERE last_restock BETWEEN '2023-04-01' AND '2024-10-01'
UNION ALL
SELECT hire_date AS event_date
FROM employees
WHERE LENGTH(email) < 14
ORDER BY event_date DESC;
