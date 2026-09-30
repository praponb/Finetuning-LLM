SELECT id, rating, country, COALESCE(rating, 0) AS rating_filled FROM suppliers WHERE country IN ('JP', 'US', 'TH') OR country = 'SG'
