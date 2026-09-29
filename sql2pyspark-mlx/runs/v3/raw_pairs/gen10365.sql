SELECT
    id,
    rating,
    supplier_name,
    city,
    SUBSTRING(supplier_name, 1, 4) AS supplier_name_prefix
FROM suppliers
WHERE NOT (UPPER(supplier_name) LIKE '%IAM%' AND country IN ('TH', 'SG'))
ORDER BY supplier_name, 1;
