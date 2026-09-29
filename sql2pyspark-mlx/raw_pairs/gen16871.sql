SELECT supplier_name FROM suppliers WHERE supplier_name IS NOT NULL UNION SELECT supplier_name FROM suppliers WHERE LENGTH(supplier_name) < 5
