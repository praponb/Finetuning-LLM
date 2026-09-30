SELECT country, TRIM(supplier_name) AS clean_supplier_name FROM suppliers WHERE LENGTH(supplier_name) < 12
