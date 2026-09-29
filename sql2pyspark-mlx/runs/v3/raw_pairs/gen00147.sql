SELECT id, rating, DENSE_RANK() OVER (ORDER BY rating DESC) AS rating_dense_rank FROM suppliers WHERE rating >= 4.2 ORDER BY rating_dense_rank ASC
