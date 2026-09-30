SELECT id, supplier_name, city FROM suppliers WHERE rating >= 2.3 UNION SELECT id, supplier_name, city FROM suppliers WHERE LENGTH(supplier_name) > 8
