SELECT id, rating, country, ROW_NUMBER() OVER (ORDER BY rating DESC, id) rn, LAG(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) prev_rating FROM suppliers WHERE country = 'UK'
