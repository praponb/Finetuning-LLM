SELECT id, country, supplier_name
FROM suppliers
WHERE supplier_name NOT LIKE '%Who%'
UNION ALL
SELECT id, country, supplier_name
FROM suppliers
WHERE UPPER(supplier_name) LIKE '%IAM%'
ORDER BY id;
