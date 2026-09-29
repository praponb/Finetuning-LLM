SELECT country FROM customers UNION SELECT country FROM suppliers WHERE LOWER(supplier_name) LIKE '%oky%';
