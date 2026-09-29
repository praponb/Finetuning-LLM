SELECT country, supplier_name FROM suppliers WHERE LENGTH(supplier_name) > 7 UNION ALL SELECT country, supplier_name FROM suppliers WHERE UPPER(supplier_name) LIKE '%URO%'
