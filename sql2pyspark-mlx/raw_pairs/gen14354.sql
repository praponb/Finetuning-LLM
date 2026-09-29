SELECT id, supplier_name
FROM suppliers
WHERE supplier_name IS NOT NULL
UNION ALL
SELECT id, supplier_name
FROM suppliers
WHERE city IS NOT NULL
ORDER BY supplier_name;
