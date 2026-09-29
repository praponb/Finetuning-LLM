SELECT id, supplier_name, country, INITCAP(TRIM(supplier_name)) AS supplier_name_title FROM suppliers ORDER BY id LIMIT 5;
