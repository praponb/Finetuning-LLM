SELECT id, country, supplier_name FROM suppliers WHERE city NOT IN ('London', 'Berlin') UNION ALL SELECT id, country, supplier_name FROM suppliers WHERE rating <= 2.8;
