SELECT supplier_name FROM suppliers WHERE rating <= 2.0 EXCEPT SELECT supplier_name FROM suppliers WHERE country IN ('JP', 'DE', 'US');
