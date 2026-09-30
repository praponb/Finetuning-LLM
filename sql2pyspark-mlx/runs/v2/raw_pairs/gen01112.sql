SELECT city, MAX(rating) AS max_rating FROM suppliers WHERE supplier_name NOT LIKE '%EuroGo%' AND city = 'London' GROUP BY city HAVING COUNT(rating) >= 2;
