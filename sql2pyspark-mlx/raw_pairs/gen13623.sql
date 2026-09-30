SELECT id, rating, city, DENSE_RANK() OVER (ORDER BY rating DESC) AS rating_dense_rank, AVG(rating) OVER (PARTITION BY city) AS city_avg_rating FROM suppliers
