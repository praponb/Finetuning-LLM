SELECT rating, country, city, INITCAP(TRIM(supplier_name)) AS supplier_name_title FROM suppliers WHERE UPPER(supplier_name) LIKE '%ION%' OR country = 'JP'
