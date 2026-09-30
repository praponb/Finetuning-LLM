SELECT city, country, MIN(rating) min_rating FROM suppliers WHERE country != 'JP' GROUP BY city, country ORDER BY min_rating DESC, country
