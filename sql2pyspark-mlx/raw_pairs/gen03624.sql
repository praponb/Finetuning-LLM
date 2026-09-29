SELECT 'shipment' AS origin, delivered_date
FROM shipments
WHERE status = 'DELIVERED'
UNION
SELECT 'employee' AS origin, hire_date
FROM employees
WHERE job_title != 'Intern'
