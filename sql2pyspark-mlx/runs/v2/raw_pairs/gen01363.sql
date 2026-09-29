SELECT id, supplier_name, TRIM(supplier_name) AS clean_supplier_name, LOWER(supplier_name) AS supplier_name_lower FROM suppliers WHERE supplier_name LIKE 'Eu%';
