SELECT *
FROM suppliers
WHERE supplier_name LIKE 'Li%'
ORDER BY country, id
LIMIT 20;
