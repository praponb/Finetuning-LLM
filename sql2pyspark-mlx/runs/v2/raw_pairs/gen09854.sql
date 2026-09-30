SELECT *, SUBSTRING(supplier_name, 1, 3) AS supplier_name_prefix
FROM suppliers
WHERE supplier_name IS NOT NULL
ORDER BY rating DESC
