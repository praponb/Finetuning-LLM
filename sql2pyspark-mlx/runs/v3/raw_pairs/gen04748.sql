SELECT id, supplier_name, city
FROM suppliers
WHERE city <> 'Chiang Mai'
UNION ALL
SELECT id, supplier_name, city
FROM suppliers
WHERE supplier_name IS NULL;
