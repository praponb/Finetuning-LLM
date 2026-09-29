SELECT city, country, SUBSTRING(city, 1, 2) AS city_prefix FROM suppliers WHERE country = 'TH' ORDER BY city ASC
