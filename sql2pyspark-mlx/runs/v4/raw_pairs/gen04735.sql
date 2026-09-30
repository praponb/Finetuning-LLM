SELECT country
FROM suppliers
EXCEPT
SELECT country
FROM suppliers
WHERE supplier_name LIKE 'Sia%';
