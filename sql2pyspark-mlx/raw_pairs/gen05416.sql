SELECT country
FROM suppliers
WHERE city = 'Tokyo'
UNION
SELECT country
FROM suppliers
WHERE LENGTH(supplier_name) >= 10;
