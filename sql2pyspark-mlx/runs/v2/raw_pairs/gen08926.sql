SELECT id, supplier_name, city FROM suppliers WHERE country = 'TH' UNION ALL SELECT id, supplier_name, city FROM suppliers WHERE UPPER(TRIM(supplier_name)) = 'SIAM PARTS CO'
