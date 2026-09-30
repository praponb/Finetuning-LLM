SELECT *, FLOOR(rating) AS rating_whole, UPPER(city) AS city_upper FROM suppliers WHERE country = 'TH' AND city != 'Berlin';
