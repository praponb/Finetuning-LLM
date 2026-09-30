SELECT id, city, rating, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating) AS rating_dense_rank FROM suppliers WHERE city = 'Chiang Mai'
