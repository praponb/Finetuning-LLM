SELECT carrier AS label, 'shipment' AS source
FROM shipments
UNION
SELECT dept_name AS label, 'department' AS source
FROM departments
WHERE dept_name IS NULL;
