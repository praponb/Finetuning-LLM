SELECT delivered_date AS event_date
FROM shipments
EXCEPT
SELECT hire_date AS event_date
FROM employees
ORDER BY event_date;
