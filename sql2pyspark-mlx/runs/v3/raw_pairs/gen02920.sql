SELECT id, supplier_name, country
FROM suppliers
WHERE rating BETWEEN 2.4 AND 3.6
UNION
SELECT id, supplier_name, country
FROM suppliers
WHERE rating BETWEEN 1.9 AND 4.1;
