SELECT country
FROM suppliers
WHERE rating >= 4.0
UNION ALL
SELECT country
FROM suppliers
WHERE supplier_name LIKE 'Glo%'
