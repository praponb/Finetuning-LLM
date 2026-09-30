SELECT id, country, supplier_name
FROM suppliers
WHERE rating < 2.2
EXCEPT
SELECT id, country, supplier_name
FROM suppliers
WHERE rating >= 2.2
ORDER BY id DESC;
