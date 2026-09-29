SELECT id, country FROM suppliers WHERE supplier_name NOT LIKE '%Who%' UNION SELECT id, country FROM suppliers WHERE city = 'Tokyo' ORDER BY id;
