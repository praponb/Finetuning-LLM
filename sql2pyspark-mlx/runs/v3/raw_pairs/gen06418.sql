SELECT id, rating, country FROM suppliers WHERE rating < 2.7 AND supplier_name NOT LIKE '%Par%' ORDER BY rating DESC;
