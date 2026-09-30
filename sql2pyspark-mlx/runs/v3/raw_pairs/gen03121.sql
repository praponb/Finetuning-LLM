SELECT rating, country, TRIM(supplier_name) AS clean_supplier_name, TRIM(supplier_name) AS clean_supplier_name_2 FROM suppliers WHERE country = 'US' OR rating < 4.0
