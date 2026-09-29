SELECT id, supplier_name, TRIM(supplier_name) clean_supplier_name, TRIM(city) clean_city FROM suppliers WHERE city NOT IN ('Bangkok', 'Chiang Mai', 'Berlin')
