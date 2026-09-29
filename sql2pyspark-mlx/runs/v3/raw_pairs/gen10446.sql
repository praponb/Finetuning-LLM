SELECT id, supplier_name, UPPER(supplier_name) AS supplier_name_upper FROM suppliers WHERE city IN ('Tokyo', 'Bangkok')
