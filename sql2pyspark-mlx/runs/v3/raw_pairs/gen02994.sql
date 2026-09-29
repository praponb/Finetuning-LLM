SELECT city, 'supplier' AS source
FROM suppliers
WHERE supplier_name LIKE '%Toky%'
UNION ALL
SELECT location AS city, 'department' AS source
FROM departments
WHERE location = 'London'
