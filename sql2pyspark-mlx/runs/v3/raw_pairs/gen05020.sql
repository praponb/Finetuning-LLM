SELECT supplier_name, CONCAT(supplier_name, ', ', country) AS supplier_name_country FROM suppliers WHERE country != 'TH' OR UPPER(supplier_name) LIKE '%ION%' ORDER BY supplier_name DESC
