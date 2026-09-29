SELECT city, supplier_name
FROM suppliers
WHERE supplier_name LIKE '%Pacif%'
UNION
SELECT city, supplier_name
FROM suppliers
WHERE city NOT IN ('New York', 'Bangkok', 'Berlin')
