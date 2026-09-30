SELECT *, COALESCE(rating, 0) AS rating_filled FROM suppliers WHERE city = 'Chiang Mai' ORDER BY rating_filled
