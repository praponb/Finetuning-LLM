SELECT supplier_name FROM suppliers WHERE city IN ('New York', 'Bangkok') UNION ALL SELECT supplier_name FROM suppliers WHERE rating > 3.3 ORDER BY supplier_name DESC;
