SELECT level FROM employees INTERSECT SELECT supplier_name AS level FROM suppliers WHERE supplier_name LIKE '%Gmb%';
