SELECT city FROM suppliers WHERE supplier_name LIKE 'T%' INTERSECT SELECT city FROM stores WHERE city = 'Singapore' ORDER BY city
