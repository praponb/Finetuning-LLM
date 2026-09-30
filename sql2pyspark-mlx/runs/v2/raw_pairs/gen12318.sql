SELECT country FROM customers WHERE email LIKE '%@GMAIL.COM' INTERSECT SELECT country FROM suppliers WHERE supplier_name LIKE 'To%';
