SELECT country
FROM suppliers
WHERE city <> 'Tokyo' AND UPPER(supplier_name) LIKE '%URO%'
ORDER BY country;
