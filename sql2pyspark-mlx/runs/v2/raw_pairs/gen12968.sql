SELECT city, country
FROM suppliers
WHERE LENGTH(supplier_name) < 7
UNION
SELECT city, country
FROM suppliers
WHERE LENGTH(supplier_name) >= 12
