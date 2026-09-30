SELECT hire_date
FROM employees
WHERE hire_date IS NOT NULL
UNION
SELECT order_date
FROM orders
WHERE order_date IS NOT NULL
ORDER BY hire_date DESC
