SELECT city, MAX(rating) AS max_rating FROM suppliers WHERE country IN ('US', 'SG') AND supplier_name NOT LIKE '%Pacific%' GROUP BY city HAVING MAX(rating) >= 4.2;
