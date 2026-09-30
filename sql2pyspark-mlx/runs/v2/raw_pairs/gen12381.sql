SELECT city FROM suppliers WHERE LENGTH(supplier_name) > 6 UNION SELECT city FROM stores WHERE opened_date >= '2023-03-01' ORDER BY city
