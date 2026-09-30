SELECT id, supplier_name, COALESCE(supplier_name, 'N/A') AS supplier_name_filled, INITCAP(TRIM(country)) AS country_title FROM suppliers WHERE city IN ('Berlin', 'Chiang Mai', 'London')
