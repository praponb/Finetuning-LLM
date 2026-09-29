SELECT *, UPPER(supplier_name) AS supplier_name_upper, SUBSTRING(supplier_name, 1, 2) AS supplier_name_prefix
FROM suppliers
WHERE supplier_name LIKE '%Parts%' OR LOWER(supplier_name) LIKE '%uro%'
ORDER BY rating DESC
