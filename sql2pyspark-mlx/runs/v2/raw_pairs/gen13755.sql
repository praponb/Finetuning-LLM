SELECT *, COALESCE(rating, 0) AS rating_filled FROM suppliers WHERE rating >= 3.9 AND rating < 2.2
