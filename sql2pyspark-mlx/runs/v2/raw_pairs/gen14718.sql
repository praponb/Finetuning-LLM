SELECT city FROM suppliers INTERSECT SELECT city FROM stores WHERE store_name NOT LIKE 'Sia%' ORDER BY city
