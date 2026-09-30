SELECT * FROM suppliers WHERE LENGTH(supplier_name) < 14 AND country NOT IN ('SG', 'UK')
