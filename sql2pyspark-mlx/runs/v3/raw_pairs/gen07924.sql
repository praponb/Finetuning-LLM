SELECT city, supplier_name FROM suppliers WHERE rating >= 2.5 EXCEPT SELECT city, supplier_name FROM suppliers WHERE city NOT IN ('Tokyo', 'Berlin', 'Bangkok');
