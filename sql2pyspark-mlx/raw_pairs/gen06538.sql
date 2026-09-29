SELECT id, city, CONCAT(country, ', ', supplier_name) AS country_supplier_name
FROM suppliers
WHERE UPPER(TRIM(supplier_name)) = 'GLOBAL SUPPLY' AND supplier_name LIKE '%Tok%'
