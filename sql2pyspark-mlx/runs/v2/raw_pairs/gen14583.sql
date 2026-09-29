SELECT id, city, rating, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rating_rank FROM suppliers WHERE country = 'US'
