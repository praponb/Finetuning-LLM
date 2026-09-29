SELECT p.id, p.price, s.supplier_name, s.rating, s.country FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE UPPER(TRIM(s.supplier_name)) = 'EUROGOODS GMBH'
