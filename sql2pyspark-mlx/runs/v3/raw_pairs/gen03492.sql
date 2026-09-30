SELECT city FROM customers INTERSECT SELECT city FROM suppliers WHERE LENGTH(supplier_name) >= 7
