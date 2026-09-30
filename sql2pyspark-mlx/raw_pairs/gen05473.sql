SELECT hire_date event_date
FROM employees
WHERE email IS NOT NULL
UNION ALL
SELECT order_date event_date
FROM orders
WHERE discount >= 0.14
ORDER BY event_date
