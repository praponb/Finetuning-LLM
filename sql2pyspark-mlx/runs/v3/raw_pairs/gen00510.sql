SELECT delivered_date
FROM shipments
WHERE carrier = 'FedEx'
UNION ALL
SELECT hire_date AS delivered_date
FROM employees
