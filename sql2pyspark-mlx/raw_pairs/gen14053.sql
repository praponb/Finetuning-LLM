SELECT id, rating, city, DENSE_RANK() OVER (ORDER BY rating DESC) AS dense_rnk, LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating FROM suppliers WHERE country = 'US';
